var co = require('co');
var sqlite3 = require('sqlite3');
var extend = require('extend-merge').extend;
var merge = require('extend-merge').merge;
var Database = require('chaos-database').Database;
var SqliteDialect = require('sql-dialect').Sqlite;
var connect = require('rqlite-js/lib/api/data/client')

var toPlainJs = require('rqlite-js/lib/api/results').toPlainJs
var getError = require('rqlite-js/lib/api/results').getError


/**
 * SQLite adapter
 */
class Sqlite extends Database {
  /**
   * Check for required PHP extension, or supported database feature.
   *
   * @param  String  feature Test for support for a specific feature, i.e. `"transactions"`
   *                         or `"arrays"`.
   * @return Boolean         Returns `true` if the particular feature is supported, `false` otherwise.
   */
  static enabled(feature) {
    var features = {
      arrays: false,
      transactions: true,
      savepoints: true,
      booleans: true,
      default: false
    };
    if (!arguments.length) {
      return extend({}, features);
    }
    return features[feature];
  }

  /**
   * Constructs the SQLite adapter and sets the default port to 3306.
   *
   * @param Object config Configuration options for this class. Available options
   *                      defined by this class:
   *                      - `'host'`: _string_ The IP or machine name where SQLite is running,
   *                                  followed by a colon, followed by a port number or socket.
   *                                  Defaults to `'localhost'`.
   */
  constructor(config) {
    var defaults = {
      classes: {
        dialect: SqliteDialect
      },
      database: undefined,
      mode : sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
      connect: true,
      alias: true,
      client: undefined,
      dialect: true
    };
    config = merge({}, defaults, config);
    super(config);

    /**
     * Specific value denoting whether or not table aliases should be used in DELETE and UPDATE queries.
     *
     * @var Boolean
     */
    this._alias = config.alias;

    /**
     * Stores a connection to a remote resource.
     *
     * @var Function
     */
    this._client = config.client;

    /**
     * Whether the client is connected or not.
     *
     * @var Boolean
     */
    this._connected = false;

    this.formatter('datasource', 'boolean', function(value, options) {
      return value ? '1' : '0';
    });

    if (typeof this._dialect === 'object') {
      return;
    }

    var dialect = this.classes().dialect;

    this._dialect = new dialect({
      caster: function(value, states) {
        var type;
        if (states && states.schema) {
          type = states.schema.type(states.name);
        }
        type = type ? type : this.constructor.getType(value);
        return this.convert('datasource', type, value);
      }.bind(this)
    });
  }

  /**
   * Returns the client instance.
   *
   * @return Function
   */
  client() {
    return this._client;
  }

  /**
   * Connects to the database using the options provided to the class constructor.
   *
   * @return boolean Returns `true` if a database connection could be established,
   *                 otherwise `false`.
   */
  connect() {
    if (this._client) {
      return Promise.resolve(this._client);
    }

    var config = this.config();

    if (!config.database) {
      return Promise.reject(new Error('Error, no database name has been configured.'));
    }

    var self = this;

    return new Promise(function(accept, reject) {

      var client = self._client

      connect(config.database).then(function(api) {
        client = self._client = api
        self._connected = true;
        accept(client)
      }).catch(function (error) {
        reject(error)
      })
    });
  }

  /**
   * Checks the connection status of this data source.
   *
   * @return Boolean Returns a boolean indicating whether or not the connection is currently active.
   *                 This value may not always be accurate, as the connection could have timed out or
   *                 otherwise been dropped by the remote resource during the course of the request.
   */
  connected() {
    return this._connected;
  }

  /**
   * Opens a transaction
   *
   * @return Promise
   */
  openTransaction() {
    return this.execute('BEGIN TRANSACTION');
  }

  /**
   * Finds records using a SQL query.
   *
   * @param  string sql  SQL query to execute.
   * @param  array  data Array of bound parameters to use as values for query.
   *                     WARNING data must be clean at this step. SQL injection must be handled earlier.
   * @return object      A `Cursor` instance.
   */

  query(sql, data, options) {


    var self = this;
    return new Promise(function(accept, reject) {

      var defaults = {};
      options = extend({}, defaults, options);

      var cursor = self.constructor.classes().cursor;

      var response = function(err, data, lastId) {
        if (err) {
          reject(err);
          return;
        }

        if (typeof lastId!== undefined) {
          self._lastInsertId = lastId;
        }

        accept(data ? new cursor({ data: data }) : true);

      };

      // Thanks node-sqlite3 for such crappy API SQL !
      var method;
      var pure = false

      self.connect().then(function(client) {

        if (sql instanceof String) {

        } else {
          console.log(sql)
          sql = sql.toString()
        }

        console.log(sql)


        if (sql.match(/^(SELECT|PRAGMA)/i)) {
          method = client.select
        }
        if (sql.match(/^(INSERT)/i)) {
          method = client.insert
        }

        if (sql.match(/^(UPDATE)/i)) {
          method = client.update
        }

        if (sql.match(/^(DELETE)/i)) {
          method = client.delete
        }

        if (sql.match(/^(CREATE TABLE)/i)) {
          method = client.table.create
        }

        if (sql.match(/^(DROP TABLE)/i)) {
          method = client.table.drop
        }

          method(sql).then(function(res) {

              var error = getError(res.body.results);
              var result = toPlainJs(res.body.results);


              var lastID = res.body.results[0].last_insert_id;

              if (sql.match(/^(SELECT \"sql\" FROM \"sqlite_master\")/i)) {
                var returnRes = toPlainJs(res.body.results)
                console.log(returnRes)
                accept(returnRes)
              }

              if (method === client.insert) {
                response(error, false, lastID)
              } else {
                response(error, result, lastID)
              }


          }).catch(function (error) {
            response(error, null, undefined)
          })

      });
    });
  }

  /**
   * Execute a raw query.
   *
   * @param  string  sql SQL query to execute.
   * @return Promise
   */
  execute(sql) {
    this.query(sql);
  }


  /**
   * Returns the last insert id from the database.
   *
   * @return mixed Returns the last insert id.
   */
  lastInsertId() {
    return this._lastInsertId;
  }

  /**
   * Returns the list of tables in the currently-connected database.
   *
   * @return Object Returns an object of sources to which models can connect.
   */

  sources() {

    var select = this.dialect().statement('select');

    select.fields('name')
      .from('sqlite_master')
      .where({ type: 'table' });
    return this._sources(select);
  }

  /**
   * Extracts fields definitions of a table.
   *
   * @param  String name The table name.
   * @return Object      The fields definitions.
   */
  fields(name) {
    return co(function*() {
      var tmp, fields = [];


      var select = this.dialect().statement('select');
      var sql = select.fields('sql')
        .from('sqlite_master')
        .where({ type: 'table' })
        .where({ tbl_name: name });

      var columns = yield this.query(sql);
      for (var column of columns) {
        var field = this._field(column);
        var dflt = column.dflt_value;

        switch (field.type) {
          case 'string':
            var matches = typeof dflt === 'string' ? dflt.match(/^'(.*)'/) : null;
            if (matches) {
              dflt = matches[1];
            }
            break;
          case 'boolean':
            dflt = dflt === '1';
            break;
          case 'datetime':
            dflt = dflt !== 'CURRENT_TIMESTAMP' ? dflt : null;
            break;
        }

        tmp = {};
        tmp[column.name] = extend({}, {
          null: (column.notnull === 0 ? true : false),
          'default': dflt
        }, field);

        fields.push(tmp);
      }
      return fields;
    }.bind(this));
  }

  /**
   * Converts database-layer column to a generic field.
   *
   * @param  Object column Database-layer column.
   * @return Object        A generic field.
   */
  _field(column) {
    var matches = column.type.match(/(\w+)(?:\(([\d,]+)\))?/);
    var field = {};
    field.type = matches[1];
    field.length = matches[2];
    field.use = field.type;

    if (field.length) {
      var length = field.length.split(',');
      field.length = Number.parseInt(length[0]);
      if (length[1]) {
        field.precision = Number.parseInt(length[1]);
      }
    }

    field.type = this.dialect().mapped(field);
    return field;
  }

  /**
   * Disconnects the adapter from the database.
   *
   * @return Boolean Returns `true` on success, else `false`.
   */
  disconnect() {
    if (!this._client) {
      return true;
    }
    // this._client.close();
    this._client = undefined;
    this._connected = false;
    return true;
  }
}

module.exports = Sqlite;
