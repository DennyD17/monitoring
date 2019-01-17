# -*- coding: utf-8 -*-

find_failed_workflows_query = """
SELECT 
    failed.*, 
    pv.value as username,
    pv2.value as queueName
    FROM (SELECT 
            l.id as load_id,
            l.alive,
            max(ls.status),
            l.xid,
            max(ls.effective_from),
            max(wf.name),
            max(wf.engine),
            max(wf.category),
            max(wf.inf_app_name),
            max(wf.inf_project),
            max(wf.inf_folder),
            max(wf.inf_workflow),
            min(l.start_dttm),
            bool_and(wf.scheduled),
            max(wf.id)
        FROM loading l
        JOIN loading_status ls ON 1=1 
            AND l.id=ls.loading_id 
        JOIN wf ON 1=1 
            AND l.wf_id = wf.id 
        WHERE 1=1 
            AND wf.name in (%s)
            AND ls.status = 'ERROR'
            AND (now()::timestamp - ls.effective_from::timestamp) < '%s' 
        GROUP BY load_id
         ) as failed    
 LEFT JOIN param_value pv on 1=1
    AND failed.load_id=pv.loading_id
    AND pv.param='user.name'
 LEFT JOIN param_value pv2 on 1=1
    AND failed.load_id=pv2.loading_id
    AND pv2.param='queueName';
"""

find_failed_workflows_query_old = """
SELECT 
    failed.*, 
    pv.value as username,
    pv2.value as queueName
    FROM (SELECT 
            l.id as load_id,
            l.alive,
            max(ls.status),
            l.xid,
            max(ls.effective_from),
            max(wf.name),
            max(wf.engine),
            max(wf.category),
            max(wf.inf_app_name),
            max(wf.inf_project),
            max(wf.inf_folder),
            max(wf.inf_workflow),
            min(l.start_dttm),
            bool_and(wf.scheduled)
        FROM loading l
        JOIN loading_status ls ON 1=1 
            AND l.id=ls.loading_id 
        JOIN wf ON 1=1 
            AND l.wf_id = wf.id 
        WHERE 1=1 
            AND wf.name in (%s)
            AND (l.alive = 'ABORTED' 
                OR ls.status in ('ERROR') 
                )
            AND (now()::timestamp - ls.effective_from::timestamp) < '%s' 
        GROUP BY load_id
         ) as failed    
 LEFT JOIN param_value pv on 1=1
    AND failed.load_id=pv.loading_id
    AND pv.param='user.name'
 LEFT JOIN param_value pv2 on 1=1
    AND failed.load_id=pv2.loading_id
    AND pv2.param='queueName';
"""