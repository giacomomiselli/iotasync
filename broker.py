# broker.py
#   sub:topic            -> sottoscrive il client al topic
#   pub:topic:messaggio  -> invia il messaggio a tutti i sottoscritti al topic

import asyncio
import random
import threading
import time


class Genero(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.t = 0
        self.u = 0
    def getu(self):
        return self.u
    def gett(self):
        return self.t
    def run(self):
        while True:
            self.t = random.randint(0, 40)
            self.u = random.randint(0, 100)
            time.sleep(random.randint(1, 30))

class Broker():
    def __init__(self, g):
        self.host = "0.0.0.0"
        self.port = 5000
        self.subscribers = {}   # topic -> lista dei writer dei client sottoscritti
        self.led = False

    async def pubblica_temperatura(self, writer):
        try:
            writer.write(f"OK {g.gett()} C\n".encode())
            await writer.drain()
        except ConnectionError:
            print("ERR")

    async def pubblica_umidita(self, writer):
        try:
            writer.write(f"OK {g.getu()}%\n".encode())
            await writer.drain()
        except ConnectionError:
            print("ERR")

    async def accendi_led(self, writer):
        self.led = True
        try:
            writer.write(f"OK\n".encode())
            await writer.drain()
        except ConnectionError:
            print("ERR")

    async def spegni_led(self, writer):
        self.led = False
        try:
            writer.write(f"OK\n".encode())
            await writer.drain()
        except ConnectionError:
            print("ERR")

    async def stato(self, writer, indirizzo):
        try:
            writer.write(f"OK {g.gett()} C, {g.getu()}%, led {self.led}\n".encode())
            await writer.drain()
        except ConnectionError:
            print("[ERRORE] Invio fallito a un subscriber")

    async def esegui_comando(self, comando, writer, indirizzo):
        comando = comando.lower()
        if comando == 'temp':
            await self.pubblica_temperatura(writer)

        elif comando == 'umidita':
            await self.pubblica_umidita(writer)

        elif comando == 'led on':
            await self.accendi_led(writer)

        elif comando == 'led off':
            await self.spegni_led(writer)

        elif comando == 'stato':
            await self.stato(writer, indirizzo)

        elif comando == 'quit':
            writer.close()
            await writer.wait_closed()

        else:
            print(f"[ERRORE] Comando sconosciuto: {comando}")

    async def gestisci_client(self, reader, writer):
        indirizzo = writer.get_extra_info("peername")
        print(f"[CONNESSIONE] {indirizzo}")

        try:
            while True:
                dati = await reader.readline()
                if not dati:
                    break
                comando = dati.decode().strip()
                print(f"[RICEVUTO] {comando} da {indirizzo}")
                await self.esegui_comando(comando, writer, indirizzo)
        except ConnectionError:
            pass

        print(f"[DISCONNESSIONE] {indirizzo}")
        writer.close()
        await writer.wait_closed()

    async def avvia(self):
        server = await asyncio.start_server(self.gestisci_client, self.host, self.port)
        print(f"[BROKER] In ascolto su {self.host}:{self.port}")
        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    g = Genero()
    g.start()
    broker = Broker(g)
    try:
        asyncio.run(broker.avvia())
    except KeyboardInterrupt:
        print("[STOP] Broker interrotto")