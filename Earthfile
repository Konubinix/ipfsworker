VERSION 0.6

IMPORT github.com/Konubinix/Earthfile AS e

ipfsworker:
	FROM e+debian
    DO e+DEBIAN_NO_AUTO_INSTALL
	RUN apt-get update && apt-get --yes install python3-pip build-essential libpq-dev libpython3-dev curl
    DO e+USE_USER
	RUN python3 -m pip install asyncio-redis==0.16.0 aredis
	RUN python3 -m pip install aiopg==1.3.4
    WORKDIR /app
    COPY --dir ipfsworker.py ipfscontroller.py ipfsworkerlib.py ./
    CMD ["/app/ipfsworker.py"]
