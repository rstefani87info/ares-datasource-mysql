/**
 * @author Roberto Stefani
 **/

import mysql from "mysql";
import {
  SQLDBConnection,
} from "@ares/core/datasources.js";
import { asyncConsole } from "@ares/core/console.js";
import { dataDescriptors } from "@ares/core/dataDescriptors.js";

export class MariaDB extends SQLDBConnection {
  constructor(
    connectionParameters,
    datasource,
    sessionId,
    connectionSettingName
  ) {
    super(connectionParameters, datasource, sessionId, connectionSettingName);
    this.pool = this.datasource.getPool(this.connectionSettingName, () => 
      mysql.createPool({ ...this, multipleStatements: true })
    );
  }

  async nativeConnect(callback) {
    console.log("creating MariaDB instance");
    const sessionId = this.sessionId;
    const MariaDBpool = this.pool;
    this.connection = this.connection ?? null;
    if (!this.connection) {
      const dbConn = this;
      this.connection = await new Promise((resolve, reject) => {
        MariaDBpool.getConnection((err, conn) => {
          console.log("Connecting MariaDB " + this.sessionId);
          if (err) {
            console.error("Error getting connection:", err);
            return reject(err);
          }
          conn.on("end", () => {
            delete dbConn.datasource.sessions[sessionId];
          });
          callback(err);
          resolve(conn);
        });
      });
    }
  }

  nativeDisconnect() {
    this.connection.release((releaseError) => {
      if (releaseError) {
        console.error("Error releasing connection:", releaseError);
        return;
      }
      delete this.datasource.sessions[this.sessionId];
    });
  }

  startTransaction(name) {
    const thisInstance = this;
    if ( !this.transaction) {
      console.log("Starting transaction: " + name + ' on ' + this.sessionId);
      this.connection.beginTransaction((transactionError) => {
        if (transactionError) {
          throw new Error("Error on starting transaction: " + transactionError);
        }
        thisInstance.transaction = name;
      });
    }
  }

  rollback(name) {
    if (this.transaction === name) {
      this.connection.rollback();
      this.transaction = null;
    }
  }

  commit(name) {
    if (this.transaction === name) {
      this.connection.commit((commitError) => {
        if (commitError) {
          throw new Error(
            'Error on committing transaction "' + name + '": ' + commitError
          );
        } else this.transaction = null;
      });
    }
  }

  async executeNativeQueryAsync(command, params ) {
    const date = new Date();
    const response = { executionTime: date.getTime(), executionDateTime: date };
    const connectionHandler = this;
    if (!this.datasource.aReS.isProduction()) {
      response.query = command;
      response.params = params.parameters;
    }
    return await new Promise(async (resolve, reject) => {
        connectionHandler.connection.query(
          command,
          params,
          (error, results, fields) => {
            response.executionTime =
              new Date().getTime() - response.executionTime;
            response.fields = fields;
            response.results = results;
            response.error = error;
  
            if (error) {
              reject(response);
            } else {
              resolve(response);
            }
          }
        );
    });
  }
  executeQuerySync(command, params, callback) {
    const date = new Date();
    const response = { executionTime: date.getTime(), executionDateTime: date };
    const logName = "executeQuerySync_" + response.executionTime;
    asyncConsole.log(logName, "Waiting for query results:");
    let wait = true;
    this.connection.query(command, params, (error, results, fields) => {
      wait = false;
      response.fields = fields;
      response.results = results;
      response.error = error;
      callback(response);
    });
    while (wait) {
      asyncConsole.log(logName, ".....");
      setTimeout(() => {}, 100);
    }
    return response;
  }
}

export const MARIADBDataTypeDescriptors = {
    binary: (length = 65535) => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^[01]+$") ,
      maxLength: length,
      minLength: 1
    }),
  
    longblob: (length = 4294967295) => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^[\\s\\S]*$"),
      maxLength: length,
      minLength: 1
    }),
  
    longtext: (length = 4294967295) => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^[\\s\\S]*$"),
      maxLength: length,
      minLength: 1
    }),
  
    mediumtext: (length = 16777215) => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^[\\s\\S]*$"),
      maxLength: length,
      minLength: 1
    }),
  
    set: (values = []) => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^(" + values.join("|") + ")+$"),
    }),
  
    smallint: (signable, precision = 3) => ({
      ...dataDescriptors.number,
      pattern: new RegExp("^" + (signable ? '[+\\-]{0,1}' : '') + "[0-9]{1," + precision + "}$"),
      max: signable ? 32767 : 65535,
      min: signable ? -32768 : 0,
      maxLength: precision,
      minLength: 1
    }),
  
    tinyint: (signable, precision = 3) => ({
      ...dataDescriptors.number,
      pattern: new RegExp("^" + (signable ? '[+\\-]{0,1}' : '') + "[0-9]{1," + precision + "}$"),
      max: signable ? 127 : 255,
      min: signable ? -128 : 0,
      maxLength: precision,
      minLength: 1
    }),
  
    tinyblob: (length = 255) => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^[\\s\\S]*$"),
      maxLength: length,
      minLength: 1
    }),
  
    tinytext: (length = 255) => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^.{0,255}$"),
      maxLength: length,
    }),
  
    varbinary: (length = 65535) => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^[01]{" + length + "}$"),
      maxLength: length,
    }),
  
    varchar: (length = 255) => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^.{0," + length + "}$"),  // Lunghezza massima
      maxLength: length,
      minLength: 1
    }),
  
    bigint: (signable, length = 20) => ({
      ...dataDescriptors.number,
      pattern: new RegExp("^" + (signable ? '[+\\-]{0,1}' : '') + "[0-9]{1," + length + "}$"),
      max: signable ? 9223372036854775807 : 18446744073709551615,
      min: signable ? -9223372036854775808 : 0,
      maxLength: length,
      minLength: 1
    }),
  
    int: (signable, length = 11) => ({
      ...dataDescriptors.number,
      pattern: new RegExp("^" + (signable ? '[+\\-]{0,1}' : '') + "[0-9]{1," + length + "}$"),
      max: signable ? 2147483647 : 4294967295,
      min: signable ? -2147483648 : 0,
      maxLength: length,
      minLength: 1
    }),
  
    decimal: (signable, precision = 10, scale = 2) => ({
      ...dataDescriptors.number,
      pattern: new RegExp("^" + (signable ? '[+\\-]{0,1}' : '') + "[0-9]{1," + (precision - scale) + "}" + (scale > 0 ? "\\.[0-9]{1," + scale + "}" : "") + "$"),
      maxLength: precision + scale + 1,
      minLength: 1
    }),
  
    float: (signable, precision = 10, scale = 2) => ({
      ...dataDescriptors.number,
      pattern: new RegExp("^" + (signable ? '[+\\-]{0,1}' : '') + "[0-9]{1," + (precision - scale) + "}" + (scale > 0 ? "\\.[0-9]{1," + scale + "}" : "") + "$"),
      maxLength: precision + scale + 1,
      minLength: 1
    }),
  
    double: (signable, precision = 15, scale = 5) => ({
      ...dataDescriptors.number,
      pattern: new RegExp("^" + (signable ? '[+\\-]{0,1}' : '') + "[0-9]{1," + (precision - scale) + "}" + (scale > 0 ? "\\.[0-9]{1," + scale + "}" : "") + "$"),
      maxLength: precision + scale + 1,
      minLength: 1
    }),
  
    char: (length = 255) => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^.{1," + length + "}$"),
      maxLength: length,
      minLength: length,
    }),
  
    text: () => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^[\\s\\S]*$"),
      maxLength: 65535,
      minLength: 1
    }),
  
    date: () => ({
      ...dataDescriptors.date,
    }),
  
    datetime: () => ({
      ...dataDescriptors.date,
    }),
  
    timestamp: () => ({
      ...dataDescriptors.date,
    }),
  
    time: () => ({
      ...dataDescriptors.date,
    }),
  
    datetime2: (precision = 6) => ({
      ...dataDescriptors.date,
      pattern: new RegExp("^(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})\\.\\d{" + precision + "}$")
    }),
  
    enum: (values = []) => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^(" + values.join("|") + ")$")
    }),
  
    boolean: () => ({
      ...dataDescriptors.boolean,
    }),
  
    // json: () => ({
    //   ...dataDescriptors.text,
    //   pattern: new RegExp("^\\{.*\\}$")
    // }),
  
    blob: (length = 65535) => ({
      ...dataDescriptors.text,
      pattern: new RegExp("^.*$"),
      maxLength: length,
      minLength: 1
    }),
  
    mediumint: (signable, length = 8) => ({
      ...dataDescriptors.number,
      pattern: new RegExp("^" + (signable ? '[+\\-]{0,1}' : '') + "[0-9]{1," + length + "}$"),
      maxLength: length,
      minLength: 1
    }),
}
