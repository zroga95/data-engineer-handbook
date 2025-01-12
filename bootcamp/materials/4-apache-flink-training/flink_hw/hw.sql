select * from sink_table

--Q1
select host, AVG(CHAR_LENGTH(url_list) - CHAR_LENGTH(REPLACE(url_list, ',', ''))) AS avg_per_session
from sink_table
where host LIKE '%techcreator.io'
group by host;

--Q2
select host, AVG(CHAR_LENGTH(url_list) - CHAR_LENGTH(REPLACE(url_list, ',', ''))) AS avg_per_session
from sink_table
where host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
group by host
ORDER BY avg_per_session DESC;