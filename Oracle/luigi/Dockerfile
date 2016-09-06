FROM centos

COPY oracle-instantclient12.1-* /tmp/
RUN yum -y install epel-release && yum -y update && yum -y install python-setuptools python-devel \
    git supervisor && yum -y localinstall /tmp/oracle-instantclient12.1-* && \
    easy_install pip && pip install luigi && \
    mkdir /usr/lib/oracle/12.1/client64/network/admin -p

COPY tnsnames.ora /usr/lib/oracle/12.1/client64/network/admin/
COPY client.sh /etc/profile.d/
COPY ./supervisor_luigi.ini /etc/supervisord.d/luigi.ini
COPY pcornetcdm.ini /tmp/

EXPOSE 8082
CMD ["/usr/bin/supervisord", "-n"]