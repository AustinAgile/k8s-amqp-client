'use strict';
var amqplib = require('amqplib');

var rabbit = {
	connection: null,
	channel: null
};

module.exports = {
	closeConnection: function () {
		return new Promise(function (resolve, reject) {
			console.log("Checking for existing rabbit connection.");
			if (rabbit.connection) {
				console.log("Trying close an existing rabbit connection...");
				rabbit.connection.close().then(function () {
					console.log("...Rabbit connection closed.");
					rabbit.connection = null;
					resolve();
				}).catch(function (error) {
					resolve();
				});
			} else {
				console.log("No existing rabbit connection...");
				resolve();
			}
		});
	},

	getConnection: function (url, retry, k8sProbes) {
		console.log("Trying to open a rabbit connection...");
		var options = {
			frameMax: "0x1000",
			channelMax: 10,
		};
		var promise = amqplib.connect(url, options);
		var then = promise.then(function (connection) {
			console.log("...got a rabbit connection.");
			rabbit.connection = connection;
			k8sProbes.setReadiness(true);
			connection.on('close', function () {
				k8sProbes.setReadiness(false);
				console.log("Rabbit connection closed.");
				retry();
			});
			connection.on('error', function (err) {
				console.log("Rabbit connection error.");
				console.log(err);
				k8sProbes.setReadiness(false);
				connection.close.bind(connection);
				rabbit.connection = null;
			});
			//return connection;
			if (typeof connection !== 'undefined') {
				console.log("Connection is good");
				return connection;
			} else {
				console.log("Connection undefined");
			}
		});
		then.catch(function (error) {
			console.log("...error trying to get a rabbit connection.");
			//console.log(error);
			k8sProbes.setReadiness(false);
			retry();
		});
		return then;
	},

	closeChannel: function () {
		return new Promise(function (resolve, reject) {
			console.log("Checking for existing rabbit channel.");
			if (rabbit.channel) {
				console.log("Trying close an existing rabbit channel...");
				rabbit.channel.close().then(function () {
					console.log("...Rabbit channel closed.");
					rabbit.channel = null;
					resolve();
				}).catch(function (error) {
					resolve();
				});
			} else {
				console.log("No existing rabbit channel...");
				resolve();
			}
		});
	},

	createChannel: function (connection) {
		console.log("Trying to get a rabbit channel...");
		var promise = connection.createChannel();
		var then = promise.then(function (channel) {
			console.log("...got a rabbit channel.");
			rabbit.channel = channel;
			return channel;
		});
		then.catch(function (err) {
			console.log("...error trying to get a rabbit channel.");
			console.log(err);
		});
		return then;
	},

	assertQueue: function (channel, queue) {
		console.log("Trying to assert the in queue...");
		var options = {
			durable: true,
			exclusive: false,
			autoDelete: false,
			deadLetterExchange: null,
			maxLength: null,
			maxPriority: null,
			arguments: {}
		};
		var promise = channel.assertQueue(queue, options)
		var then = promise.then(function (result) {
			console.log("...asserted the in queue.");
			return result;
		});
		then.catch(function (err) {
			console.log("...error trying to assert the in queue.");
			console.log(err);
		});
		return then;
	},

	assertExchange: function (channel, exchange) {
		console.log("Trying to assert the "+exchange+" exchange...");
		var options = {
			durable: true,
			internal: false,
			autoDelete: false,
			alternateExchange: null,
			arguments: {}
		};
		var promise = channel.assertExchange(exchange, "direct", options)
		var then = promise.then(function (result) {
			console.log("...asserted the the "+exchange+" exchange.");
			return result;
		});
		then.catch(function (err) {
			console.log("...error trying to assert the the "+exchange+" exchange.");
			console.log(err);
		});
		return then;
	},

	bindQueue: function (channel, queue, exchange) {
		console.log("Trying to bind the the "+exchange+" exchange to the "+queue+" queue...");
		var promise = channel.bindQueue(queue, exchange, "")
		var then = promise.then(function (result) {
			console.log("...bound the the "+exchange+" exchange to the "+queue+" queue.");
			return result;
		});
		then.catch(function (err) {
			console.log("Error trying to bind the the "+exchange+" exchange to the "+queue+" queue.");
			console.log(err);
			console.log("Setting timeout to bind the the "+exchange+" exchange to the "+queue+" queue...");
			setTimeout(function () {
				console.log("Timeout expired...");
				this.bindQueue(channel, queue, exchange);
			}, 1000 * amqpRetrySeconds);
		});
		return then;
	},

	consumeQueue: function (channel, queue, handler) {
		console.log("Trying to consume the "+queue+" queue...");
		var promise = channel.consume(queue, function (message) {
			handler(message, channel);
		});
		var then = promise.then(function (result) {
			console.log("...consuming the "+queue+" queue.");
			return result;
		});
		then.catch(function (err) {
			console.log("...error trying to consume the "+queue+" queue.");
			console.log(err);
		});
		return then;
	},

	cancelConsumption: function (consumerTag) {
		return new Promise(function (resolve, reject) {
			console.log("Trying to cancel consumption...");
			if (rabbit.channel) {
				rabbit.channel.cancel(consumerTag)
					.then(function (result) {
						console.log("...cancelled consumption.");
						resolve(null);
					})
					.catch(function (err) {
						console.log("...error trying to cancel consumption.");
						console.log(err);
						resolve(null);
					});
			} else {
				console.log("No existing consumption...");
				resolve();
			}
		});
	}

};
