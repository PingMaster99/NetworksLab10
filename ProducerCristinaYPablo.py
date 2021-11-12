from kafka import KafkaProducer
import json
import numpy as np
from random import choice, randrange
from time import sleep

wind_directions = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE']

producer = KafkaProducer(
    bootstrap_servers=['20.120.14.159:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def get_gaussian_random(decimals=0, var=16.67):
    # 3 * 16.67 is about 50. 3 Standard deviations represent about 99.7% of all values
    random_number = np.random.normal(50, var)
    if random_number < 0:
        random_number = 0.00
    elif random_number > 100:
        random_number = 100.00
    if decimals != 0:
        return round(random_number, decimals)
    else:
        return int(random_number)


for i in range(100):
    temperature = get_gaussian_random(decimals=2)
    humidity = get_gaussian_random(var=25)
    wind_direction = choice(wind_directions)
    short_temperature = int(temperature)
    wind_index = wind_directions.index(wind_direction)

    data = {'temperature': temperature, 'humidity': humidity, 'wind_direction': wind_direction}
    compact_data = f'{chr(short_temperature)}{chr(humidity)}{chr(wind_index)}'
    compact_data.encode('ASCII')
    print(data)
    print(compact_data)
    producer.send('18259', compact_data)    # Change compact_data to data to send the full payload
    producer.flush()
    sleep_time = randrange(15, 30)
    print('Sending more data in:', sleep_time, 'seconds')
    sleep(sleep_time)














#
# for _ in range(10):
#     producer.send('18259', {"id": str(uuid.uuid4())})
#     producer.flush()
