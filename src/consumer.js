const { Kafka } = require('kafkajs');
const dotenv = require('dotenv'); dotenv.config();

const PlaylistSongsService = require('./PlaylistSongsService');
const MailSender = require('./MailSender');
const Listener = require('./listener');

const init = async () => {
  const playlistSongsService = new PlaylistSongsService();
  const mailSender = new MailSender();
  const listener = new Listener(playlistSongsService, mailSender);

  // ----

  const kafka = new Kafka({
    brokers: [process.env.KAFKA_SERVER]
  })

  const consumer = kafka.consumer({
    groupId: "export_playlistSongs-nodejs"
  })

  await consumer.connect()
  await consumer.subscribe({
    topic: "export_playlistSongs",
    fromBeginning: true
  })

  await consumer.run({
    eachMessage: async (record) => {
      const message = record.message;
      listener.listen(message.value)
      console.info(message.value);
    }
  })

  // ----

  // const connection = await amqp.connect(process.env.RABBITMQ_SERVER);
  // const channel = await connection.createChannel();
  // await channel.assertQueue('export:playlistSongs', {
  //   durable: true,
  // });

  // await channel.consume('export:playlistSongs', listener.listen, { noAck: true });
};

init();
