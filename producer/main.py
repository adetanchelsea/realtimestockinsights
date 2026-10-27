from extract import connect_to_api, extract_json
from producer_setup import init_producer, topic

def main():
    response = connect_to_api()

    data = extract_json(response)

    producer = init_producer()

    # Specify the actual response from the extract json function
    for stock in data:
        result = {
            'date': stock['date'],
            'symbol': stock['symbol'],
            'open': stock['open'],
            'low': stock['low'],
            'high': stock['high'],
            'close': stock['close']
        }

        print (result)

    return None

if __name__ == '__main__':
    main()