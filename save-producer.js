var { Kafka, logLevel } = require('kafkajs');
var http = require('axios');
var fs = require("fs");

var baseurl = "http://localhost:3000/";

var _kafka = new Kafka({
  clientId: 'testapp',
  brokers: ['kafka:9092'],
  logLevel: logLevel.INFO
});


const producer = _kafka.producer({
    allowAutoTopicCreation: true,
    transactionTimeout: 60000
});

const sentData = {"stormId": "2345678", "terminalId": "848548Y0", "businessName": "Some business ventures", "externalReference": "8794fj48943", "ourReference": "2384j43r583498", "destinationAccount": "0009217408", "transactionStatus": "Pending", "amount": 1500, "transactionType": "Cash-Out", "description": "withdraw some money", "accountType": "Wallet", "dateCreated": "2022-03-21", "aggregatorId": "50-4987", "mid": 0};
const payload = JSON.stringify(sentData);

const run = async () => {
    await producer.connect();
    await producer.send({
    topic: 'nibss.gateway.make.payment',
    messages: [
        { value: payload },
    ],
    });

    await producer.disconnect();
}

setInterval(() => {
    run().catch(e => {
        const errdata = `${new Date()} - [testapp] ${e.message}\n`;
        fs.writeFile('prodSaverrdata.txt', errdata, { flag: 'a+' }, err => {});
      
    });
}, 3000);

/*
const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list': 'kafka1:9092'
}, {}, {
    topic: ''
});

stream.on('error', (err) => {
    console.error('Error in our kafka stream');
    console.error(err);
});

function queueRandomMessage() {
    const sentData = {"stormId": "2345678", "terminalId": "848548Y0", "businessName": "Some business ventures", "externalReference": "8794fj48943", "ourReference": "2384j43r583498", "destinationAccount": "0009217408", "transactionStatus": "Pending", "amount": 1500, "transactionType": "Cash-Out", "description": "withdraw some money", "accountType": "Wallet", "dateCreated": "2022-03-21", "aggregatorId": "50-4987", "mid": 0};
    const payload = JSON.stringify(sentData);
    const success = stream.write(Buffer.from(payload));
    if (success) {
        console.log("Message wrote to stream successfully!");
    } else {
        console.log("Something went wrong");
    }
}


setInterval(() => {
    queueRandomMessage();
}, 3000);
*/