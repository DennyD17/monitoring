set mapred.job.queue.name=root.external.g_dc_y_external_vk;

select archivesunpackpasscount, archivesunpackfailcount, archivesparquetpasscount, archivesparquetfailcount, ctl_loading from external_${SOURCE_NAME}_qa.statistics where ctl_loading = ${CTL_LOADING};
select ctl_loading, archivename, message, filename, filesize, goodrecords, corruptrecords from
(select ctl_loading, archivename, message, fl.filename, fl.filesize, fl.goodrecords, fl.corruptrecords from 
(select ctl_loading, p.archivename, p.message, p.filesinfolist from external_${SOURCE_NAME}_qa.statistics
lateral view explode(filesparquetpassedlist) passed as p where ctl_loading = ${CTL_LOADING}
) t1 lateral view outer explode (filesinfolist) fileslist as fl
union all                                          
select ctl_loading, archivename, message, fl.filename, fl.filesize, fl.goodrecords, fl.corruptrecords from 
(select ctl_loading, p.archivename, p.message, p.filesinfolist from external_${SOURCE_NAME}_qa.statistics
lateral view explode(filesparquetfailedlist) passed as p where ctl_loading = ${CTL_LOADING}
) t1 lateral view outer explode (filesinfolist) fileslist as fl) final;
