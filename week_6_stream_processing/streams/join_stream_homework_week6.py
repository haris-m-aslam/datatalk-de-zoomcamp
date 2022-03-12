import faust
from taxi_rides import TaxiRide


app = faust.App('datatalksclub.stream.v2', broker='kafka://localhost:9092')
topic1 = app.topic('datatalkclub.yellow_taxi_ride.json', value_type=TaxiRide)
topic2 = app.topic('datatalkclub.yellow_taxi_ride.json', value_type=TaxiRide)

s1 = app.stream(topic1)
s2 = app.stream(topic2)


@app.agent(topic1)
async def process(stream):
    async for value in (s1 & s2):
    #async for event in stream.group_by(TaxiRide.vendorId):
        print(value)


if __name__ == '__main__':
    app.main()
