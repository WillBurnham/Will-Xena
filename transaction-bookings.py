import threading
import math
import random
import string
import psycopg2
# import sys

#Imports username and password from password file
with open('password.txt') as file:
    lines = [line.rstrip() for line in file]

#fetching username and password, opening db connection
username = lines[0]
pg_password = lines[1]
conn = psycopg2.connect(database = "COSC3380", user = username, password = pg_password)
cursor = conn.cursor()

f = open("trans.txt", "r")

workers = []
lines = []
book_refs = {}
ticket_numbers = {}

passenger_name = "Clark Kent"
fare_conditions = "Economy"
total_amount = 400.00

num_threads = 2
lock = threading.Lock()

#preprocessing the input function
for line in f:
    line = line.replace(" ", "")
    if '\n' in  line and line != '\n':
        lines.append(line[0:len(line) - 1])
    elif line == '\n':
        continue
    else:
        lines.append(line)
    
#getting rid of line 0
lines.pop(0)
length = len(lines)

class Worker:
    def __init__(self, name):
        self.name = name
        self.thread = threading.Thread(target=thread_func, args=(self,lock,lines,))

def generateBookRef():
    book_ref = " "
    while " " in book_ref or (book_ref in book_refs):
        book_ref = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
    book_refs[book_ref] = True
    return book_ref

def generateTicketNo():
    ticket_no = " "
    while " " in ticket_no or (ticket_no in ticket_numbers):
        ticket_no = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(13))
    ticket_numbers[ticket_no] = True
    return ticket_no

def insertBookings(flight_id):
    book_ref = generateBookRef()
    cursor.execute("BEGIN;")
    cursor.execute(("INSERT INTO bookings (book_ref, book_date, total_amount) SELECT %s, ((flights.scheduled_departure) - INTERVAL '1 DAY'), 400.00 FROM flights WHERE flights.flight_id = %s;"), [book_ref, flight_id])
    cursor.execute("COMMIT;")
    cursor.execute(("SELECT * FROM bookings WHERE book_ref = %s;"), [book_ref])
    result = cursor.fetchone()
    print("INSERT INTO bookings (book_ref, book_date, total_amount) SELECT " + book_ref + ", ((flights.scheduled_departure) - INTERVAL '1 DAY'), 400.00 FROM flights WHERE flights.flight_id = " + flight_id + ";")  
    print(result)
    
def thread_func(worker, lock, lines):
    lock.acquire()
    for i in range(math.ceil(length / num_threads)):
        line = splitLine(lines[0])
        ticket_number = line[0]
        flight_id = line[1]
        if lines == []:
            break
        insertBookings(flight_id)
        print(worker.name)
        lines.pop(0)
    lock.release()

#getting the passenger and flight id from the line of input
def splitLine(line):
    passenger_id = ""
    flight_id = ""
    comma = False
    for char in line:
        if char == ',':
            comma = True
            continue
        if not comma:
            passenger_id += char
        else:
            flight_id += char
    return (passenger_id, flight_id)

for _ in range(num_threads):
    worker = Worker(_)
    worker.thread.start()
    workers.append(worker)

for worker in workers:
    worker.thread.join
