FROM centos

COPY oracle-instantclient12.1-* /tmp/
RUN yum -y install epel-release && yum -y update && yum -y install python-setuptools python-devel \
    git supervisor mysql-devel && yum -y localinstall /tmp/oracle-instantclient12.1-* && \
    easy_install pip && easy_install -U setuptools && yum -y groupinstall 'Development Tools' && \
    pip install airflow && pip install airflow[hive] && pip install airflow[mysql] && \
    mkdir /usr/lib/oracle/12.1/client64/network/admin -p && airflow initdb

COPY tnsnames.ora /usr/lib/oracle/12.1/client64/network/admin/
COPY client.sh /etc/profile.d/
COPY ./supervisor_airflow.ini /etc/supervisord.d/airflow.ini
RUN mkdir /root/airflow/dags && ln -s /i2p-transform-oracle/airflow/PCORNetCDM.py /root/airflow/dags/ && \
    airflow trigger_dag PCORNetCDM --run_id="`date`"

EXPOSE 8080
CMD ["/usr/bin/supervisord", "-n"]