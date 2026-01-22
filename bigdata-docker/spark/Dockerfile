FROM apache/spark:3.5.0

USER root

# Installer Python + pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Installer les libs Python nécessaires
RUN pip3 install hdfs pandas

# Revenir à l'utilisateur spark
USER spark
