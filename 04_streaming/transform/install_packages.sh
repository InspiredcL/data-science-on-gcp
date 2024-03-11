#!/bin/bash

# A la fecha de 2024-03-11 (AAAA-MM-DD) las versiones de las dependencias
# de apache-beam, no coinciden con las de timezonefinder ni pytz por ende
# se ha decidido quitar el comando --upgrade y agregar numba como dependencia
# para optimizar los cálculos

python3 -m pip install --upgrade pip
python3 -m pip cache purge
python3 -m pip install 'apache-beam[gcp]' timezonefinder[pytz] timezonefinder[numba]
python3 -m pip install --upgrade apache-beam[gcp]

echo "If this script fails, please try installing it in a virtualenv"
echo "virtualenv ~/.beam-04"
echo "source ~/.beam-04/bin/activate"
echo "./install_packages.sh"
