import * as consts from "./constants";
import crypto from "crypto";
import { MYSQL_TYPE_VARCHAR } from "./constants";

export * as consts from "./constants";

export default class Server {
 constructor(opts) {
  Object.assign(this, opts);

  if (!this.banner) this.banner = `MyServer/1.0`;
  if (!this.salt) this.salt = crypto.randomBytes(20);
  this.sequence = 0;
  this.onPacket = this.helloPacketHandler;
  this.incoming = [];

  this.socket.on(`data`, this.handleData);
  this.socket.on(`end`, this.handleDisconnect);
  this.socket.on(`error`, function(err) {
   console.log(`Error: `, err);
  });

  this.sendServerHello();
 }

 handleDisconnect = () => {
  console.log(`disconnect`);
 };

 writeHeader(data, len) {
  data.writeUIntLE(len - 4, 0, 3);
  data.writeUInt8(this.sequence++ % 256, 3);
 }

 sendPacket(payload) {
  return this.socket.write(payload, `utf8`);
 }

 newDefinition(params) {
  const type = params.type ? params.type : consts.MYSQL_TYPE_VARCHAR;
  return {
   catalog: params.catalog ? params.catalog : `def`,
   schema: params.db,
   table: params.table,
   orgTable: params.orgTable,
   name: params.name,
   orgName: params.orgName,
   columnLength: params.length ? params.length : type === MYSQL_TYPE_VARCHAR ? 255 : 0,
   type,
   flags: params.flags ? params.flags : 0,
   decimals: params.decimals,
   default: params[`default`]
  };
 }

 sendFieldList(definitions, includeDefaults) {
  // Write each definition
  let payload, len;
  for (let definition of definitions) {
   payload = Buffer.alloc(512);
   len = 4;
   for (let field of [`catalog`, `schema`, `table`, `orgTable`, `name`, `orgName`]) {
    let val = definition[field] || ``;
    len = writeLengthCodedString(payload, len, val);
   }
   len = payload.writeUInt8(0x0c, len);
   len = payload.writeUInt16LE(33, len); // ASCII
   len = payload.writeUInt32LE(definition.columnLength, len);
   len = payload.writeUInt8(definition.type, len);
   len = payload.writeUInt16LE(definition.flags ? definition.flags : 0, len);
   len = payload.writeUInt8(definition.decimals ? definition.decimals : 0, len);
   len = payload.writeUInt16LE(0, len); // \0\0 FILLER
   if (includeDefaults) {
    len = writeLengthCodedString(payload, len, definition[`default`]);
   }
   this.writeHeader(payload, len);
   this.sendPacket(payload.slice(0, len));
  }
  this.sendEOF();
 }

 sendQueryResponse(columns, rows) {
  if (columns.length === 0) {
   this.sendOK({});
  } else {
   this.sendDefinitions(columns);
   this.sendRows(rows);
  }
 }

 sendDefinitions(definitions) {
  // Write Definition Header
  let payload = Buffer.alloc(1024);
  let len = 4;
  len = writeLengthCodedBinary(payload, len, definitions.length);
  this.writeHeader(payload, len);
  this.sendPacket(payload.slice(0, len));
  this.sendFieldList(definitions, true);
 }

 sendRow(row) {
  let payload = Buffer.alloc(1024);
  let len = 4;
  for (let cell of row) {
   if (cell == null) {
    len = payload.writeUInt8(0xfb, len);
   } else {
    len = writeLengthCodedString(payload, len, cell);
   }
  }
  this.writeHeader(payload, len);
  this.sendPacket(payload.slice(0, len));
 }

 sendRows(rows = []) {
  for (let row of rows) {
   this.sendRow(row);
  }
  this.sendEOF();
 }

 sendEOF({ warningCount = 0, serverStatus = consts.SERVER_STATUS_AUTOCOMMIT } = {}) {
  // Write EOF
  let payload = Buffer.alloc(16);
  let len = 4;
  len = payload.writeUInt8(0xfe, len);
  len = payload.writeUInt16LE(warningCount, len);
  len = payload.writeUInt16LE(serverStatus, len);
  this.writeHeader(payload, len);
  this.sendPacket(payload.slice(0, len));
 }

 sendServerHello = () => {
  //## Sending Server Hello...
  let payload = Buffer.alloc(128);
  let pos = 4;
  pos = payload.writeUInt8(10, pos); // Protocol version

  pos += payload.write(this.banner || `MyServer/1.0`, pos);
  pos = payload.writeUInt8(0, pos);

  pos = payload.writeUInt32LE(process.pid, pos);

  pos += this.salt.copy(payload, pos, 0, 8);
  pos = payload.writeUInt8(0, pos);

  pos = payload.writeUInt16LE(
    consts.CLIENT_LONG_PASSWORD |
    consts.CLIENT_CONNECT_WITH_DB |
    consts.CLIENT_PROTOCOL_41 |
    consts.CLIENT_SECURE_CONNECTION,
    pos
  );

  if (this.serverCharset) {
   pos = payload.writeUInt8(this.serverCharset, pos);
  } else {
   pos = payload.writeUInt8(0x21, pos); // latin1
  }
  pos = payload.writeUInt16LE(consts.SERVER_STATUS_AUTOCOMMIT, pos);
  payload.fill(0, pos, pos + 13);
  pos += 13;

  pos += this.salt.copy(payload, pos, 8);
  pos = payload.writeUInt8(0, pos);
  this.writeHeader(payload, pos);

  return this.sendPacket(payload.slice(0, pos));
 };

 handleData = data => {
  if (data && data.length > 0) {
   this.incoming.push(data);
  }
  this.gatherIncoming();
  if (data == null) {
   console.log(`Connection closed`);
   this.socket.destroy();
  }
 };

 gatherIncoming() {
  let incoming;
  if (this.incoming.length > 0) {
   let len = 0;
   for (let buf of this.incoming) {
    len += buf.length;
   }
   incoming = Buffer.alloc(len);
   len = 0;
   for (let buf of this.incoming) {
    len += buf.copy(incoming, len);
   }
  } else {
   incoming = this.incoming[0];
  }
  let remaining = this.readPackets(incoming);
  this.incoming = [Buffer.from(remaining)];
 }

 readPackets(buf) {
  let offset = 0;
  while (true) {
   let data = buf.slice(offset);
   if (data.length < 4) return data;

   let packetLength = data.readUIntLE(0, 3);
   if (data.length < packetLength + 4) return data;

   this.sequence = data.readUInt8(3) + 1;
   offset += packetLength + 4;
   let packet = data.slice(4, packetLength + 4);

   this.onPacket(packet);
   this.packetCount++;
  }
 }

 helloPacketHandler = packet => {
  //## Reading Client Hello...

  // http://dev.mysql.com/doc/internals/en/the-packet-header.html

  if (packet.length == 0) return this.sendError({ message: `Zero length hello packet` });

  let ptr = 0;

  let clientFlags = packet.slice(ptr, ptr + 4);
  ptr += 4;

  let maxPacketSize = packet.slice(ptr, ptr + 4);
  ptr += 4;

  this.clientCharset = packet.readUInt8(ptr);
  ptr++;

  let filler1 = packet.slice(ptr, ptr + 23);
  ptr += 23;

  let usernameEnd = packet.indexOf(0, ptr);
  let username = packet.toString(`ascii`, ptr, usernameEnd);
  ptr = usernameEnd + 1;

  let scrambleBuff;

  let scrambleLength = packet.readUInt8(ptr);
  ptr++;

  if (scrambleLength > 0) {
   this.scramble = packet.slice(ptr, ptr + scrambleLength);
   ptr += scrambleLength;
  }

  let database;

  let databaseEnd = packet.indexOf(0, ptr);
  if (databaseEnd >= 0) {
   database = packet.toString(`ascii`, ptr, databaseEnd);
  }
  this.onPacket = null;

  return Promise.resolve(this.onAuthorize({ clientFlags, maxPacketSize, username, database }))
    .then(authorized => {
     if (!authorized) throw `Not Authorized`;

     this.onPacket = this.normalPacketHandler;
     this.gatherIncoming();
     this.sendOK({});
    })
    .catch(err => {
     console.log(err);
     this.sendError({ message: `Authorization Failure` });
     this.socket.destroy();
    });
 };

 normalPacketHandler(packet) {
  if (packet == null) throw `Empty packet`;
  return this.onCommand({
   command: packet.readUInt8(0),
   extra: packet.length > 1 ? packet.slice(1) : null
  });
 }
 sendOK({ message, affectedRows = 0, insertId, warningCount = 0 }) {
  let data = Buffer.alloc((message == null ? 0 : message.length) + 128);
  let len = 4;
  len = data.writeUInt8(0, len);
  len = writeLengthCodedBinary(data, len, affectedRows);
  len = writeLengthCodedBinary(data, len, insertId);
  len = data.writeUInt16LE(consts.SERVER_STATUS_AUTOCOMMIT, len);
  len = data.writeUInt16LE(warningCount, len);
  len = writeLengthCodedString(data, len, message);

  this.writeHeader(data, len);
  this.sendPacket(data.slice(0, len));
  console.log(`Send OK`);
 }

 sendError({ message = `Unknown MySQL error`, errno = 2000, sqlState = `HY000` }) {
  //## Sending Error ...
  console.log(message);
  let data = Buffer.alloc(message.length + 64);
  let len = 4;
  len = data.writeUInt8(0xff, len);
  len = data.writeUInt16LE(errno, len);
  len += data.write(`#`, len);
  len += data.write(sqlState, len, 5);
  len += data.write(message, len);
  len = data.writeUInt8(0, len);

  this.writeHeader(data, len);
  this.sendPacket(data.slice(0, len));
 }
}
function writeLengthCodedString(buf, pos, str) {
 if (str == null) return buf.writeUInt8(0, pos);
 if (typeof str !== `string`) {
  //Mangle it
  str = str.toString();
 }
 const byteLen = Buffer.byteLength(str, `utf8`);
 pos = writeLengthCodedBinary(buf, pos, byteLen);
 pos += buf.write(str, pos, byteLen);
 return pos;
}

function writeLengthCodedBinary(buf, pos, number) {
 if (number == null) {
  return buf.writeUInt8(0xfb, pos);
 } else if (number < 251) {
  return buf.writeUInt8(number, pos);
 } else if (number < 65536) {
  pos = buf.writeUInt8(0xfc, pos);
  return buf.writeUInt16LE(number, pos);
 } else if (number < 16777216) {
  pos = buf.writeUInt8(0xfd, pos);
  return buf.writeUInt32LE(number, pos);
 } else {
  pos = buf.writeUInt8(0xfe, pos);
  return buf.writeUIntLE(number, pos);
 }
}
