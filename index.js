const cors = require('cors')
const express = require('express')
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: [
	  'my-kafka-0.my-kafka-headless.bryanvre.svc.cluster.local:9092'
	  ]
});

const producer = kafka.producer()

const app = express();
app.use(cors());
app.options('*', cors());

const port = 8080;

app.get('/', (req, res, next) => {
  res.send('kafka api - BryanVRe');
});


// topic username username=name
const run = async (username) => {

    await producer.connect()
//    await producer.send()
    await producer.send({
      topic: 'test',
      messages: [ 
	{ 
	  'value': `{"name": "${username}" }` 
  	} 
      ],
    })
   await producer.disconnect()
}

app.get('/like', (req, res, next) => {
  const username = req.query.name;
  res.send({ 'name' : username } );
  run(username).catch(e => console.error(`[example/producer] ${e.message}`, e))

});
//node - topic reaction - uId=userId, oId=objectId, rId=reactionId
const ruun = async (uId, oId, rId) => {

  await producer.connect()
//    await producer.send()
  await producer.send({
    topic: 'reactions',
    messages: [ 
{ 
  'value': `{ "userId": "${uId}",  "objectId": "${oId}", "reactionId": "${rId}"}` 
  } 
    ],
  })
 await producer.disconnect()
}

app.get('/reaction', (req, res, next) => {
const uId = req.query.userId;
const oId = req.query.objectId;
const rId = req.query.reactionId;
res.send({'userId:': uId, 'objectId': oId,'reactionId' : rId } );
ruun(uId, oId, rId).catch(e => console.error(`[example/producer] ${e.message}`, e))

});

//node - topic comments    uId=userId, oId=objectId, comment=message
const r = async (uId, oId, comment) => {

  await producer.connect()
//    await producer.send()
  await producer.send({
    topic: 'comments',
    messages: [ 
  { 
    'value': `{ "userId": "${uId}",  "objectId": "${oId}", "comment": "${comment}"}`
  } 
    ],
  })
 await producer.disconnect()
}

app.get('/comments', (req, res, next) => {
const uId = req.query.userId;
const oId = req.query.objectId;
const comment = req.query.comment;
res.send({'userId:': uId, 'objectId': oId,'comment' : comment} );
r(uId, oId, comment).catch(e => console.error(`[example/producer] ${e.message}`, e))

});



app.listen(port,  () => 
	console.log('listening on port ' + port
));
