./mediasoup-demo -node CN-Edge -region CN -cluster Beijing -port 8080 -worker ./mediasoup-worker

./mediasoup-demo -node US-A-Edge -region US-A -cluster NewYork -port 8081 -worker ./mediasoup-worker

./mediasoup-demo -node US-B-Edge -region US-B -cluster SanFrancisco -port 8082 -worker ./mediasoup-worker

curl "http://localhost:8080/publish?stream=stream-1"
curl "http://localhost:8081/subscribe?stream=stream-1&client=client-1"
curl "http://localhost:8082/subscribe?stream=stream-1&client=client-2"

ps aux | grep mediasoup-demo
