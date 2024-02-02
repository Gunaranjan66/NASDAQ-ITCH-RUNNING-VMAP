#!.venv/bin/python
import sys
import gzip
import struct
import json,os
import ntpath
import traceback
from time import time
from struct import unpack
from collections import defaultdict


orders = {}
symbols_mapping = {}
order_fills = defaultdict(list)



# nas daq protocols buffer sizes
ALLOWED_PROTOCOLS = {'A': 35, 'B': 18, 'C': 35, 'D': 18, 'E': 30, 'F': 39, 'H': 24, 'I': 49, 'J': 34, 'K': 27, 'L': 25, 'N': 19, 'P': 43, 'Q': 39, 'R': 38, 'S': 11, 'U': 34, 'V': 34, 'W': 11, 'X': 22, 'Y': 19, 'h': 20}

market_starts,market_ends = None,None


HOUR=3.6e12
def get_hour(ns):
    return int(ns//HOUR)

def calculate_vwap(records):
    '''
        Calculate Volume-Weighted Average Price (VWAP) from trade records.
        VWAP is calculated as the cumulative typical price multiplied by volume divided by the cumulative volume.

        Args:
            records (list): A list of trade records, where each record is a tuple (quantity, price, timestamp).

        Returns:
            dict: A dictionary where the keys are timestamps and values are VWAP values at those timestamps.


        VWAP = Cumulative Typical Price x Volume/Cumulative Volume
    '''

     # Calculate the cumulative Typical Price x Volume
    trades = {}
    for record in records:
        quantity, price, hour = record
        if hour not in trades:
            trades[hour] = [0,0]   # [Total price,quantity]
        trades[hour][0]+=(price*quantity)
        trades[hour][1]+=quantity
    
    # Calculate the Cumulative Typical Price x Volume / Cumulative Volume
    vwap = {}
    cumulative_tp, cumulative_volume = 0, 0
    for hour in sorted(trades.keys()):
        cumulative_tp += trades[hour][0]
        cumulative_volume += trades[hour][1]
        if cumulative_volume:
            vwap[hour] = cumulative_tp /cumulative_volume
        else:
            vwap[hour] = 0
        
    return vwap

def parser_message(msg_type,msg):
    """
        Parses NASDAQ messages and updates relevant data structures.
        This function is designed to parse different types of NASDAQ messages, such as 'S', 'R', 'A', 'F', 'E', 'C', 'U', and 'P'
        Args:
            msg_type (str): The type of NASDAQ message ('S', 'R', 'A', 'F', 'E', 'C', 'U', or 'P').
            msg (bytes): The binary message data to be parsed.
        
    Example:
        parser_message('R', b'\x00\x01\x00\x00\x00\x00TEST1234\x00\x00\x00\x00\x00\x00')


    """
    global market_starts,market_ends,orders,order_fills
    try:
        if msg_type == 'S':
            # Parse Stock Trading Action message
            payload = struct.unpack('!HH6sc',msg)
            if payload[3] == b'Q':
                print("Market starts")
                market_starts = int.from_bytes(payload[2],'big')
            elif payload[3] == b'M':
                print("Market ends")
                market_ends = int.from_bytes(payload[2],'big')
        
        elif msg_type == 'R':
             # Parse Stock Directory message
            payload = struct.unpack('!HH6s8sccIcc2scccccIc',msg)
            stock_locate,symbol = payload[0],payload[3].decode().strip()
            symbols_mapping[int(stock_locate)] = symbol
        
        elif msg_type == 'A':
             # Parse Add Order message
            payload = struct.unpack('!HH6sQcI8sI',msg)
            order_id,price = payload[3],payload[7]/(10**4)
            orders[int(order_id)] = price
        
        elif msg_type == 'F':
            # Parse Add Order (MPID) message
            payload = struct.unpack('!HH6sQcI8sI4s',msg)
            order_id,price = payload[3],payload[7]/(10**4)
            orders[int(order_id)] = price
        
        elif msg_type == 'E' and market_starts:
            # Parse Order Executed message
            payload = struct.unpack('!HH6sQIQ',msg)
            stock_locate,timestamp,order_id,qty = payload[0],int.from_bytes(payload[2],'big'),payload[3],payload[4]
            price = orders[int(order_id)]
            order_fills[int(stock_locate)].append((qty,price,get_hour(timestamp)))
        
        elif msg_type == 'C' and market_starts:
             # Parse Order Executed with Price message
            payload = struct.unpack('!HH6sQIQcI',msg)
            if payload[6] == 'Y':
                stock_locate,timestamp,qty,price = payload[0],int.from_bytes(payload[2],'big')\
                ,payload[4],payload[7]/(10**4)
                order_fills[int(stock_locate)].append((qty,price,get_hour(timestamp)))
            
        elif msg_type == 'U':
            # Parse Order Replace (Non-Displayed) message
            payload = struct.unpack('!HH6sQQII',msg)
            order_id,price = payload[4],payload[6]/(10**4)
            # with lock: 
            orders[int(order_id)] = price

        elif msg_type == 'P' and market_starts:
            # Parse Trade message
            payload = struct.unpack('!HH6sQcI8sIQ',msg)
            stock_locate,timestamp,qty,price =  payload[0],int.from_bytes(payload[2],'big')\
                ,payload[5],payload[7]/(10**4)
            order_fills[int(stock_locate)].append((qty,price,get_hour(timestamp)))
        else: return
        
    except Exception as e:
        print(f"Error:{e} | trace: {traceback.format_exc()}")


def parse_file(file_path):

    print(f"parsing the file {file_path}")
    file = gzip.open(file_path,'rb')
    count = 0
    
    start_time = time()
    # with ThreadPoolExecutor(max_workers=3) as executor:

    print('started parsing and processing the protocols')

    while not market_ends:
        msg_type = file.read(1)
        msg_type = msg_type.decode()
        if msg_type not in ALLOWED_PROTOCOLS: continue
        # executor.submit(parser_message,(buffer_data[2:msg_len+2],lock))
        parser_message(msg_type,file.read(ALLOWED_PROTOCOLS[msg_type]))

        if count%1000000==0:
            print(f'parsing {count} records completed,exec time: {time()-start_time} sec')
            start_time = time()
        count+=1

    file.close()
        

# simple json dumper 
def json_to_file(d,file):
    d = dict(d)
    with open(file,'w+') as f:
        json.dump(d,f)


def time_it(func):
    '''
        Decorator function to measure the execution time of another function.
    '''
    def inner(*args,**kwrags):
        start_time = time()
        func(*args,**kwrags)
        print(f"Time taken to execute {func.__name__}: {time()-start_time}")
    return inner

def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


@time_it
def main():

    # ask the file path
    file_path = sys.argv[1] if len(sys.argv)>1 else None
    print(file_path)
    if not file_path:
        file_path  = input('Enter file location: ')
    while not os.path.isfile(file_path):
        print(f"InvalidFilePath: {file_path}, file not found ")
        file_path  = input('Enter file location: ')
    
    #parse the data
    parse_file(file_path)
    
    # calculate the vwap hourly 
    vwap_symbol_wise = {}
    for stock_locate,records in order_fills.items():
        symbol = symbols_mapping[stock_locate]
        vwap_symbol_wise[symbol] = calculate_vwap(records)
    file_name = path_leaf(file_path)

    #dumping the vwap result into json file as : 'symbol':{'hour1':price,'hour2':price..}
    json_to_file(vwap_symbol_wise,file_name+'.json')

if __name__ == '__main__':
    main()



