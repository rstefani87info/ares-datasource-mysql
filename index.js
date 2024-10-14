/**
 * @author Roberto Stefani
 **/

import mysql from "mysql";
import {
  SQLDBConnection,
} from "@ares/core/datasources.js";
import { asyncConsole } from "@ares/core/console.js";

export class MariaDB extends SQLDBConnection {
  constructor(
    connectionParameters,
    datasource,
    sessionId,
    connectionSettingName
  ) {
    super(connectionParameters, datasource, sessionId, connectionSettingName);
    
  }

  async nativeConnect(callback) {
    console.log("creatin MariaDB instance");
    const sessionId = this.sessionId;
    MariaDB.pool = MariaDB.pool ?? mysql.createPool({ ...this, multipleStatements: true });
    this.connection = this.connection ?? null;
    if (!this.connection) {
      const dbConn = this;
      this.connection = await new Promise((resolve, reject) => {
        MariaDB.pool.getConnection((err, conn) => {
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
      response.params = params;
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
      response.executionTime = new Date().getTime() - response.executionTime;
      callback(response);
    });
    while (wait) {
      asyncConsole.log(logName, ".....");
      setTimeout(() => {}, 100);
    }
    return response;
  }
}
