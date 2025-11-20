#!/bin/bash

# This script helps configure the ODBC driver for Impala on macOS.

# Check if Homebrew is installed
if ! command -v brew &> /dev/null
then
    echo "Homebrew not found. Please install Homebrew first."
    exit 1
fi

# Install unixODBC
brew install unixodbc

# Create odbc.ini and odbcinst.ini files
cat > /usr/local/etc/odbc.ini <<EOL
[Sample Cloudera Impala DSN]
Driver=Cloudera Impala ODBC Driver
HOST=192.168.3.191
PORT=21050
UID=hive
AuthMech=3
PWD=hive
UseSasl=0
EOL

cat > /usr/local/etc/odbcinst.ini <<EOL
[Cloudera Impala ODBC Driver]
Description=Cloudera Impala ODBC Driver
Driver=/opt/cloudera/impalaodbc/lib/universal/libclouderaimpalaodbc.dylib
EOL

echo "ODBC configuration files created successfully."
echo "Please make sure you have installed the Cloudera Impala ODBC driver and the driver path is correct in odbcinst.ini."
