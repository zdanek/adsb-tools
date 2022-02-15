import socket
import time as ttime
import csv
import sys
from os import path
from datetime import time, datetime, date
import traceback

host = '127.0.0.1'
port = 30003
analyze = True
fakeConnect = False
loop = True

icaoToCallsign = {}


def CheckFile():
    global csvFile
    if len(sys.argv) < 2:
        print("No csv passed.\nUsage:\n " + sys.argv[0] + " adsb-sbs1-file.csv")
        exit()

    csvFile = sys.argv[1]

    print("Opening source file " + csvFile)
    if not path.exists(csvFile):
        print("File not found")
        exit()


def WaitForConnection():
    global serv, conn
    if fakeConnect:
        print("Faking connection for debugging purposes")
        return
    serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serv.bind((host, port))
    serv.listen()

    print("Waiting for connections at port " + str(port))

    conn, addr = serv.accept()
    print("Got conn from " + str(addr) + ". Staring feed")


def Analyze(row):
    current_time = datetime.now().strftime("%H:%M:%S.%f")

    msgType = int(row[1])
    icao = row[4]
    if msgType in [1, 5, 6]:
        callsing = row[10]
        if callsing:
            if icao not in icaoToCallsign.keys():
                print(current_time + " Callsign " + icao + " -> " + callsing)
            else:
                print(current_time + " Callsign: " + callsing)

            icaoToCallsign[icao] = callsing

    if msgType == 3:
        alt = row[11]
        if 'H' in alt:
            alt = alt.replace('H', '')

        alt = str(float(alt) * 0.3048)
        lat = row[14]
        lon = row[15]
        if icao not in icaoToCallsign.keys():
            callsing = icao
        else:
            callsing = icaoToCallsign.get(icao)

        print(current_time + " Pos (" + callsing + ") " + lat + " " + lon + " alt: " + alt)


def FeedData():
    global file
    msgCounter = 0
    dotTimer = 0

    prevTime = None
    with open(csvFile) as file:
        msgReader = csv.reader(file, delimiter=',')
        # file.close()
        for row in msgReader:
            recvTime = time.fromisoformat(row[9])
            if prevTime is None:
                prevTime = recvTime

            delta = datetime.combine(date.today(), recvTime) - datetime.combine(date.today(), prevTime)
            deltaSecs = delta.total_seconds()
            # for testing purposes max waiting time for next message is 10s
            if deltaSecs > 10:
                deltaSecs = 10
                print("\nNext MSG in " + str(deltaSecs) + "s")

            prevTime = recvTime
            msgCounter += 1
            if dotTimer > 10:
                print(".", end='')
                sys.stdout.flush()
                dotTimer = 0
            if msgCounter % 500 == 0:
                print("\nAlready emitted " + str(msgCounter) + " messages")

            dotTimer += deltaSecs
            # print(str(deltaSecs) + " / " + str(dotTimer))
            if analyze:
                Analyze(row)

            # some data contains H in alt, I could not find relevant info but I think it's a code for different alt encoding
            # for testing purposes I just remove H so it can be processed by a listening program
            alt = row[11]
            if 'H' in alt:
                row[11] = alt.replace('H', '')

            if not fakeConnect:
                conn.send((','.join(row) + "\n").encode('utf-8'))

            ttime.sleep(deltaSecs)

    print("Processing done")
    if not fakeConnect:
        serv.shutdown(socket.SHUT_RDWR)
        serv.close()
    file.close()


def main():
    print("ADS-B SBS-1 fake server, ver 1.0")
    print("by Bartek Zdanowski :)\n")

    CheckFile()

    try:
        WaitForConnection()

        while True:
            FeedData()
            if not loop:
                break

    except KeyboardInterrupt:
        print("Closing")
        serv.shutdown(socket.SHUT_RDWR)
        serv.close()
        file.close()

    except BrokenPipeError:
        print("Client disconnected")
        serv.close()
        file.close()

    except Exception:
        traceback.print_exc(file=sys.stdout)

    exit(0)


if __name__ == '__main__':
    main()
