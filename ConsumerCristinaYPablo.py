from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
from time import strftime, localtime


temperatures = []
humidities = []
wind_directions = []
times = []
wind_direction_values = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE']

plt.ion()
fig, axs = plt.subplots(3)
axs[0].set(xlabel='Time', ylabel='Temperature')
axs[0].tick_params(labelrotation=90)
axs[1].set(xlabel='Time', ylabel='Humidity')
axs[1].tick_params(labelrotation=90)
axs[2].set(xlabel='Wind direction', ylabel='Count')
axs[2].tick_params(labelrotation=90)
plt.subplots_adjust(hspace=2.5)
fig.set_size_inches(10, 10)


def get_wind_counts():
    wind_collection = set(wind_directions)
    wind_counts = [wind_directions.count(element) for element in wind_collection]

    return list(wind_collection), wind_counts


def plot_metrics():
    wind, count = get_wind_counts()
    axs[0].plot(times, temperatures)
    axs[1].plot(times, humidities)
    axs[2].bar(wind, count)
    plt.xticks(rotation=90)
    plt.pause(0.01)
    plt.show()


consumer = KafkaConsumer('18259',
                         bootstrap_servers='20.120.14.159:9092',
                         value_deserializer=json.loads)


"""
Uncomment this for full payload receiving
"""
"""
for msg in consumer:

    message_dictionary = msg.value
    current_time = strftime('%Y-%m-%d %H:%M:%S', localtime())
    print(f"Received {current_time}: {message_dictionary}")
    temperatures.append(message_dictionary['temperature'])
    humidities.append(message_dictionary['humidity'])
    wind_directions.append(message_dictionary['wind_direction'])
    times.append(current_time)
    plot_metrics()
"""


"""
Short payloads (3 bytes)
"""
for msg in consumer:

    message_dictionary = [ord(character) for character in msg.value.encode().decode('ASCII')]
    current_time = strftime('%Y-%m-%d %H:%M:%S', localtime())
    print(f"Received {current_time}: temperature: {message_dictionary[0]}  humidity: {message_dictionary[1]} wind "
          f"index: {message_dictionary[2]}")
    temperatures.append(message_dictionary[0])
    humidities.append(message_dictionary[1])
    wind_directions.append(wind_direction_values[message_dictionary[2]])
    times.append(current_time)
    plot_metrics()

