
import { dataDescriptors } from "@ares/core/dataDescriptors.js";

function getParam(req, name) {
  return req?.parameters?.[name] ?? req?.params?.[name] ?? req?.query?.[name] ?? req?.body?.[name];
}

export async function aReSInitialize(aReS, datasourceId, options) {
	if (!aReS || typeof aReS !== "object") {
		throw new TypeError("aReSInitialize(aReS, datasourceId, options) requires an aReS instance");
	}

    const datasource = aReS.datasourceMap[datasourceId];
    if (!datasource) {
        throw new Error("aReSInitialize(aReS, datasourceId, options) requires a datasource with id " + datasourceId);
    }
	 
    const {
      receive,
      tagAsReceived: tagAsReceivedOptions,
      send,
      read,
      softDelete,
      restoreSoftDelete,
      restore,
      undelete,
      physicalDelete,
      delete: physicalDeleteAlias,
      setStatus,
      archive,
      forward,
      tableName
    } = options ?? {};
    if (!tableName) {
      throw new Error("aReSInitialize(aReS, datasourceId, options) requires options.tableName");
    }

    const tagOptions = tagAsReceivedOptions ?? receive;
    const physicalDeleteOptions = physicalDelete ?? physicalDeleteAlias;

    if (send) {
      await datasource.loadQuery(sendDefaultMapper(send, tableName));
    }
    if (read) {
      await datasource.loadQuery(readDefaultMapper(read, tableName));
    }
    if (tagOptions) {
      await datasource.loadQuery(tagAsReceived(tagOptions, tableName));
    }
    if (softDelete) {
      await datasource.loadQuery(softDeleteMessage(softDelete, tableName));
    }
    const restoreOptions = restoreSoftDelete ?? restore ?? undelete;
    if (restoreOptions) {
      await datasource.loadQuery(restoreSoftDeleteMessage(restoreOptions, tableName));
    }
    if (physicalDeleteOptions) {
      await datasource.loadQuery(physicalDeleteMessage(physicalDeleteOptions, tableName));
    }
    if (setStatus) {
      await datasource.loadQuery(setMessageStatus(setStatus, tableName));
    }
    if (archive) {
      await datasource.loadQuery(archiveMessage(archive, tableName));
    }
    if (forward) {
      await datasource.loadQuery(forwardMessage(forward, tableName));
    }

	return aReS;
}

export default aReSInitialize;


export const sendDefaultMapper = ({connectionSetting, path, name, transaction, methods, query, parametersValidationRoles, mapParameters, ...others }, tableName) => {
    return {
        name: name??'sendMessage',
        path: path??'/send-message',
        transaction : transaction ?? false,
        methods: methods??'post',
        connectionSetting: connectionSetting, 
        query: query??`INSERT INTO ${tableName} (title, body, \`from\`, from_alias, \`to\`, parent_id, topic_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        parametersValidationRoles: parametersValidationRoles ?? ((req,aReS)=>({
            title: {
                required: true,
                type: dataDescriptors.text,
                minLength: 10,
                maxLength: 500,
                source: (req, name) => getParam(req, name),
            },
            body: {
                required: true,
                type: dataDescriptors.text,
                minLength: 10,
                source: (req, name) => getParam(req, name),
            },
            from: {
                required: true,
                type: dataDescriptors.text,
                minLength: 10,
                maxLength: 500,
                source: (req, name) => getParam(req, name),
            },
            fromAlias: {
                required: true,
                type: dataDescriptors.text,
                minLength: 10,
                maxLength: 500,
                source: (req, name) => getParam(req, name),
            },
            to: {
                required: true,
                type: dataDescriptors.text,
                minLength: 10,
                maxLength: 500,
                source: (req, name) => getParam(req, name),
            },
            parentId: {
                required: false,
                type: dataDescriptors.number,
                source: (req, name) => getParam(req, name),
            },
            topicId: {
                required: true,
                type: dataDescriptors.number,
                source: (req, name) => getParam(req, name),
            },
        })),
        mapParameters : mapParameters ?? function(req,aReS) {
            return [req.parameters.title, req.parameters.body, req.parameters.from, req.parameters.fromAlias, req.parameters.to, req.parameters.parentId, req.parameters.topicId];
        },
        ...others,
    };
};

export const readDefaultMapper = ({connectionSetting, path, name, transaction, methods, query, parametersValidationRoles, mapParameters, ...others }, tableName) => {
    return {
        name: name??'readMessage',
        path: path??'/read-message',
        transaction : transaction ?? false,
        methods: methods??'get',
        connectionSetting: connectionSetting, 
        query: query??`SELECT * FROM ${tableName} WHERE topic_id = ?`,
        parametersValidationRoles: parametersValidationRoles ?? ((req,aReS)=>({
            topicId: {
                required: true,
                type: dataDescriptors.number,
                source: (req, name) => getParam(req, name),
            },
        })),
        mapParameters : mapParameters ?? function(req,aReS) {
            return [req.parameters.topicId];
        },
        ...others,
    };
};

export const tagAsReceived = ({connectionSetting, path, name, transaction, methods, query, parametersValidationRoles, mapParameters, ...others }, tableName) => {
    return {
        name: name??'tagAsReceived',
        path: path??'/tag-as-received',
        transaction : transaction ?? false,
        methods: methods??'post',
        connectionSetting: connectionSetting, 
        query: query??`UPDATE ${tableName} SET received_received = ? WHERE id = ?`,
        parametersValidationRoles: parametersValidationRoles ?? ((req,aReS)=>({
            id: {
                required: true,
                type: dataDescriptors.number,
                source: (req, name) => getParam(req, name),
            },
            
        })),
        mapParameters : mapParameters ?? function(req,aReS) {
            return [true, req.parameters.id];
        },
        ...others,
    };
};

export const softDeleteMessage = (
  { connectionSetting, path, name, transaction, methods, query, parametersValidationRoles, mapParameters, ...others },
  tableName
) => {
  return {
    name: name ?? "softDeleteMessage",
    path: path ?? "/soft-delete",
    transaction: transaction ?? false,
    methods: methods ?? "post",
    connectionSetting,
    query: query ?? `UPDATE ${tableName} SET deleted_deleted = ? WHERE id = ?`,
    parametersValidationRoles:
      parametersValidationRoles ??
      ((req, aReS) => ({
        id: {
          required: true,
          type: dataDescriptors.number,
          source: (req, name) => getParam(req, name),
        },
        deleted: {
          required: false,
          type: dataDescriptors.boolean,
          default: true,
          source: (req, name) => getParam(req, name),
        },
      })),
    mapParameters:
      mapParameters ??
      function (req, aReS) {
        return [req.parameters.deleted ?? true, req.parameters.id];
      },
    ...others,
  };
};

export const restoreSoftDeleteMessage = (
  { connectionSetting, path, name, transaction, methods, query, parametersValidationRoles, mapParameters, ...others },
  tableName
) => {
  return {
    name: name ?? "restoreSoftDeleteMessage",
    path: path ?? "/restore",
    transaction: transaction ?? false,
    methods: methods ?? "post",
    connectionSetting,
    query: query ?? `UPDATE ${tableName} SET deleted_deleted = ? WHERE id = ?`,
    parametersValidationRoles:
      parametersValidationRoles ??
      ((req, aReS) => ({
        id: {
          required: true,
          type: dataDescriptors.number,
          source: (req, name) => getParam(req, name),
        },
      })),
    mapParameters:
      mapParameters ??
      function (req, aReS) {
        return [false, req.parameters.id];
      },
    ...others,
  };
};

export const physicalDeleteMessage = (
  { connectionSetting, path, name, transaction, methods, query, parametersValidationRoles, mapParameters, ...others },
  tableName
) => {
  return {
    name: name ?? "physicalDeleteMessage",
    path: path ?? "/delete",
    transaction: transaction ?? false,
    methods: methods ?? "post",
    connectionSetting,
    query: query ?? `DELETE FROM ${tableName} WHERE id = ?`,
    parametersValidationRoles:
      parametersValidationRoles ??
      ((req, aReS) => ({
        id: {
          required: true,
          type: dataDescriptors.number,
          source: (req, name) => getParam(req, name),
        },
      })),
    mapParameters:
      mapParameters ??
      function (req, aReS) {
        return [req.parameters.id];
      },
    ...others,
  };
};

export const setMessageStatus = (
  { connectionSetting, path, name, transaction, methods, query, parametersValidationRoles, mapParameters, ...others },
  tableName
) => {
  return {
    name: name ?? "setMessageStatus",
    path: path ?? "/set-status",
    transaction: transaction ?? false,
    methods: methods ?? "post",
    connectionSetting,
    query: query ?? `UPDATE ${tableName} SET status = ? WHERE id = ?`,
    parametersValidationRoles:
      parametersValidationRoles ??
      ((req, aReS) => ({
        id: {
          required: true,
          type: dataDescriptors.number,
          source: (req, name) => getParam(req, name),
        },
        status: {
          required: false,
          type: dataDescriptors.text,
          default: "archived",
          source: (req, name) => getParam(req, name),
        },
      })),
    mapParameters:
      mapParameters ??
      function (req, aReS) {
        return [req.parameters.status ?? "archived", req.parameters.id];
      },
    ...others,
  };
};

export const archiveMessage = (settings, tableName) => {
  return setMessageStatus(
    {
      name: settings?.name ?? "archiveMessage",
      path: settings?.path ?? "/archive",
      ...settings,
    },
    tableName
  );
};

export const forwardMessage = (
  { connectionSetting, path, name, transaction, methods, query, parametersValidationRoles, mapParameters, ...others },
  tableName
) => {
  return {
    name: name ?? "forwardMessage",
    path: path ?? "/forward",
    transaction: transaction ?? false,
    methods: methods ?? "post",
    connectionSetting,
    query:
      query ??
      `INSERT INTO ${tableName} (title, body, \`from\`, from_alias, \`to\`, parent_id, topic_id)
       SELECT title, body, ?, ?, ?, id, topic_id FROM ${tableName} WHERE id = ?`,
    parametersValidationRoles:
      parametersValidationRoles ??
      ((req, aReS) => ({
        id: {
          required: true,
          type: dataDescriptors.number,
          source: (req, name) => getParam(req, name),
        },
        from: {
          required: true,
          type: dataDescriptors.text,
          minLength: 1,
          source: (req, name) => getParam(req, name),
        },
        fromAlias: {
          required: true,
          type: dataDescriptors.text,
          minLength: 1,
          source: (req, name) => getParam(req, name),
        },
        to: {
          required: true,
          type: dataDescriptors.text,
          minLength: 1,
          source: (req, name) => getParam(req, name),
        },
      })),
    mapParameters:
      mapParameters ??
      function (req, aReS) {
        return [req.parameters.from, req.parameters.fromAlias, req.parameters.to, req.parameters.id];
      },
    ...others,
  };
};
