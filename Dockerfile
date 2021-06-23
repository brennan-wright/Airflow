FROM apache/airflow

RUN pip install 'apache-airflow-providers-microsoft-azure==1.2.0rc1'
RUN pip install 'apache-airflow-providers-samba'
RUN pip install 'apache-airflow-providers-sftp'