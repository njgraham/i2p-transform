''' Airflow DAG definition
'''

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from attrdict import AttrDict
from datetime import datetime
from os import path
from subprocess import check_output, STDOUT


def _run_command(cmd):
    # Simply return the command to be run for now
    return cmd
    # return check_output(cmd, stderr=STDOUT, shell=True)


def _run_sql(ds, **kwargs):
    cli_args = kwargs['dag_run'].conf
    conf = AttrDict(kwargs['dag_run'].conf)

    connect_str = ('%(pcornet_cdm_user)s/%(pcornet_cdm_pass)s@'
                   '(DESCRIPTION=(ADDRESS='
                   '(PROTOCOL=TCP)(Host=%(host)s)(Port=%(port)s))'
                   '(CONNECT_DATA=(SID=%(sid)s)))' % conf.sql_vars)

    sql = ('''connect %(connect_str)s
    set echo on;
    %(defines)s
    WHENEVER SQLERROR EXIT SQL.SQLCODE;
    start %(sql_file)s'''
           % dict(connect_str=connect_str,
                  defines='\n'.join(['define %s=%s' % (var, val)
                                     for var, val in
                                     cli_args['sql_vars'].items()]),
                  sql_file=path.join(conf.paths.transform_path,
                                     kwargs['sql_file'])))
    return _run_command('''sqlplus /nolog <<EOF
    %(sql)s
    EOF''' % dict(sql=sql))


def load_deps(ds, **kwargs):
    # TODO: Consider splitting this up
    conf = AttrDict(kwargs['dag_run'].conf)
    mconf = conf.sql_vars
    mconf.update(conf.paths)

    return _run_command(
        '''export pcornet_cdm_user=%(pcornet_cdm_user)s && \
        export pcornet_cdm_pass=%(pcornet_cdm_pass)s && \
        cd %(transform_path)s && \
        python load_csv.py harvest_local harvest_local.csv \
        harvest_local.ctl pcornet_cdm_user pcornet_cdm_pass && \
        python load_csv.py PMN_LabNormal pmn_labnormal.csv \
        pmn_labnormal.ctl pcornet_cdm_user pcornet_cdm_pass && \
        ./load_pcornet_mapping.sh''' % mconf)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 8, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG('PCORNetCDM', default_args=default_args, schedule_interval=None)

load_deps = PythonOperator(
    task_id='load_deps',
    provide_context=True,
    python_callable=load_deps,
    dag=dag)

prerun_tests = PythonOperator(
    task_id='prerun_tests',
    provide_context=True,
    python_callable=_run_sql,
    op_kwargs=dict(sql_file='PCORNetLoader_prerun_tests.sql'),
    dag=dag)

prerun_tests.set_upstream(load_deps)

pcornet_mapping = PythonOperator(
    task_id='pcornet_mapping',
    provide_context=True,
    python_callable=_run_sql,
    op_kwargs=dict(sql_file='pcornet_mapping.sql'),
    dag=dag)

pcornet_mapping.set_upstream(prerun_tests)

gather_table_stats = PythonOperator(
    task_id='gather_table_stats',
    provide_context=True,
    python_callable=_run_sql,
    op_kwargs=dict(sql_file='gather_table_stats.sql'),
    dag=dag)

gather_table_stats.set_upstream(pcornet_mapping)

common = PythonOperator(
    task_id='common',
    provide_context=True,
    python_callable=_run_sql,
    op_kwargs=dict(sql_file='PCORNetLoader_common.sql'),
    dag=dag)

common.set_upstream(gather_table_stats)

demographic = PythonOperator(
    task_id='demographic',
    provide_context=True,
    python_callable=_run_sql,
    op_kwargs=dict(sql_file='PCORNetLoader_demographic.sql'),
    dag=dag)

demographic.set_upstream(common)

encounter = PythonOperator(
    task_id='encounter',
    provide_context=True,
    python_callable=_run_sql,
    op_kwargs=dict(sql_file='PCORNetLoader_encounter.sql'),
    dag=dag)

encounter.set_upstream(demographic)

cdm_postproc = PythonOperator(task_id='cdm_postproc',
                              provide_context=True,
                              python_callable=_run_sql,
                              op_kwargs=dict(sql_file='cdm_postproc.sql'),
                              dag=dag)

dep_encounter = [('condition', 'PCORNetLoader_condition.sql'),
                 ('death', 'PCORNetLoader_death.sql'),
                 ('death_cause', 'PCORNetLoader_death_cause.sql'),
                 ('diagnosis', 'PCORNetLoader_diagnosis.sql'),
                 ('dispensing', 'PCORNetLoader_dispensing.sql'),
                 ('enrollment', 'PCORNetLoader_enrollment.sql'),
                 ('labresultcm', 'PCORNetLoader_labresultcm.sql'),
                 ('pcornet_trial', 'PCORNetLoader_pcornet_trial.sql'),
                 ('prescribing', 'PCORNetLoader_prescribing.sql'),
                 ('procedure', 'PCORNetLoader_procedure.sql'),
                 ('pro_cm', 'PCORNetLoader_pro_cm.sql'),
                 ('vital', 'PCORNetLoader_vital.sql')]

for task_id, sql_file in dep_encounter:
    task = PythonOperator(task_id=task_id,
                          provide_context=True,
                          python_callable=_run_sql,
                          op_kwargs=dict(sql_file=sql_file),
                          dag=dag)
    encounter.set_downstream(task)
    task.set_downstream(cdm_postproc)


harvest = PythonOperator(task_id='harvest',
                         provide_context=True,
                         python_callable=_run_sql,
                         op_kwargs=dict(sql_file='PCORNetLoader_harvest.sql'),
                         dag=dag)

harvest.set_upstream(cdm_postproc)

report = PythonOperator(task_id='report',
                        provide_context=True,
                        python_callable=_run_sql,
                        op_kwargs=dict(sql_file='PCORNetLoader_report.sql'),
                        dag=dag)

report.set_upstream(harvest)

cdm_transform_tests = PythonOperator(
    task_id='cdm_transform_tests',
    provide_context=True,
    python_callable=_run_sql,
    op_kwargs=dict(sql_file='cdm_transform_tests.sql'),
    dag=dag)

cdm_transform_tests.set_upstream(report)
