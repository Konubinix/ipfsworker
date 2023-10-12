VERSION 0.7

IMPORT github.com/Konubinix/Earthfile AS e

ipfsworker:
    FROM e+debian-python-user-venv --extra_packages="python3-redis python3-aiopg python3-psycopg2 curl"
    COPY ipfsworker.py ipfscontroller.py ipfsworkerlib.py ./
    CMD ["/app/ipfsworker.py"]
