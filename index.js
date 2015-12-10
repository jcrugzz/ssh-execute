'use strict';

var sequest = require('sequest');
var util = require('util');
var bl = require('bl');
var Readable = require('readable-stream/readable');
var each = require('each-limit');

module.exports = function (options, callback) {
  if (typeof callback !== 'function') return new Executor(options);

  var execute = new Executor(options);
  execute.pipe(bl(callback));
};


util.inherits(Executor, Readable);

function Executor(options) {
  options = options || {};
  Readable.call(this);
  this.servers = options.servers || [options.server];
  if (!this.servers[0]) throw new Error('Servers required');

  this.user = options.user;
  if (!this.user) throw new Error('User required');
  //
  // Only one of these is used
  //
  this.sshOpts = {
    privateKey: options.privateKey,
    password: options.password,
    passphrase: options.passphrase
  };

  this.file = options.filename || options.file;
  this.remoteFile = options.remoteFile;
  this.command = options.command;
  if (!this.command) throw new Error('Command required');
  this.limit = options.limit || 5;

  this.connect();
}

Executor.prototype.connect = function () {
  //
  // Create a map of the connections from connection -> server
  //
  this.connections = new Map(
    this.servers.map(server => {
      var host = [this.user, server].join('@');
      return [this.listen(
        sequest.connect(host, this.sshOpts)
      ), host];
    })
  );

  if (this.file && this.remoteFile) {
    return this.copy(() => this.run());
  }

  this.run();
};

Executor.prototype.listen = function (ssh) {
  ssh.on('error', this.emit.bind(this, 'error'));
  return ssh;
};

//
// Copy the file to all the servers
//
Executor.prototype.copy = function (callback) {
  each(Array.from(this.connections.keys()), this.limit, (ssh, next) => {
    var host = this.connections.get(ssh);
    this.push('Start SFTP file ' +
      this.file + ' to ' + host + ':' + this.remoteFile + '\n');
    fs.createFileStream(this.file)
      .on('error', this.emit.bind(this, 'error'))
      .pipe(ssh.put(this.remoteFile))
      .on('close', () => {
        this.push('Finish SFTP file ' +
          this.file + ' to ' + host + ':' + this.remoteFile + '\n');
        next();
      });
  }, callback);
};

//
// Run the expected command
//
Executor.prototype.run = function () {
  each(Array.from(this.connections.keys()), this.limit, (ssh, next) => {
    ssh(this.command, (err, stdout) => {
      if (err) return next(err);
      this.push(stdout + '\n');
      next();
    });
  }, (err) => {
    if (err) return this.emit('error', err);
    this.push(null);
  });
};

