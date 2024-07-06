#!/bin/bash

# Descargar el archivo tar.gz de Quarto versión 1.5.43 para Linux (amd64) en el directorio home del usuario
curl -L -o ~/quarto-1.5.43-linux-amd64.tar.gz https://github.com/quarto-dev/quarto-cli/releases/download/v1.5.43/quarto-1.5.43-linux-amd64.tar.gz

# Crear un directorio llamado 'opt' en el directorio home del usuario
mkdir ~/opt

# Extraer el contenido del archivo descargado en el directorio 'opt'
tar -C ~/opt -xvzf ~/quarto-1.5.43-linux-amd64.tar.gz

# Crear un directorio 'bin' dentro de '.local' en el directorio home del usuario
mkdir ~/.local/bin

# Crear un enlace simbólico para el ejecutable de Quarto en el nuevo directorio bin
ln -s ~/opt/quarto-1.5.43/bin/quarto ~/.local/bin/quarto

# Agregar el nuevo directorio bin al PATH del usuario agregando una línea al archivo .profile
( echo ""; echo 'export PATH=$PATH:~/.local/bin\n' ; echo "" ) >> ~/.profile

# Recargar el archivo .profile para actualizar el PATH
source ~/.profile
