#!/bin/bash -l

cd ${APP_HOME}

echo "Entrainement du modele: START\n"
python3 modele.py
echo "Entrainement du modele: END\n"