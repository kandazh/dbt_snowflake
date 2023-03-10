-- TPC-DS_query8
select  s_store_name
      ,sum(ss_net_profit) profit
 from {{ source('tpcds_sf10tcl', 'store_sales') }}  
     ,{{ source('tpcds_sf10tcl', 'date_dim') }}  
     ,{{ source('tpcds_sf10tcl', 'store') }}  ,
     (select ca_zip
     from (
      SELECT substr(ca_zip,1,5) ca_zip
      FROM {{ source('tpcds_sf10tcl', 'customer_address') }}
      WHERE substr(ca_zip,1,5) IN (
                          '10338','56623','51423','26456','19500','65832',
                          '17178','68879','49935','49849','93956',
                          '71765','45100','50587','68389','41899',
                          '98316','56217','94686','59350','32857',
                          '14925','31266','37817','27519','20787',
                          '26967','49045','39397','32010','23144',
                          '53580','15491','74151','18442','51916',
                          '17730','22824','28290','21657','45460',
                          '39386','21133','35017','19894','21759',
                          '79293','86733','76777','41688','13810',
                          '49053','17992','13395','19869','40785',
                          '63897','65049','27388','94701','41482',
                          '97923','23951','88284','61718','94317',
                          '72294','63544','31306','41242','28830',
                          '75535','86189','88177','16147','12902',
                          '48271','54036','20936','27802','96741',
                          '70286','75710','16034','90285','22058',
                          '52590','40584','62441','64039','68999',
                          '64327','33844','52497','88495','25989',
                          '67814','13767','83194','99395','35524',
                          '89640','48834','51875','71073','25383',
                          '19129','57805','47962','61905','19557',
                          '74159','98032','13917','50936','47993',
                          '41606','17592','11470','28216','19732',
                          '97958','60997','85688','96863','16605',
                          '10898','31340','71340','72902','98949',
                          '74440','53057','30323','76166','27195',
                          '11204','32771','38189','83221','22295',
                          '15325','20844','65549','69207','71903',
                          '63929','56922','25733','75482','14986',
                          '79223','73692','98769','70275','33793',
                          '13057','30142','95737','30072','32097',
                          '25845','50282','19289','92221','59533',
                          '37375','29706','48186','22385','55809',
                          '17416','10592','55385','71829','91975',
                          '73557','38036','10448','95252','51386',
                          '14190','15247','39907','79438','78053',
                          '66623','27720','84139','74147','58637',
                          '11434','36573','10081','53536','41724',
                          '97898','36752','50384','87352','35696',
                          '69486','50026','27837','42592','58865',
                          '80523','53682','65423','77611','98529',
                          '13909','13727','52190','36152','48355',
                          '62496','16527','18143','98830','75198',
                          '73043','64043','63042','67797','50656',
                          '27700','60687','57905','94404','15733',
                          '80809','74562','84493','67977','11213',
                          '19125','84496','16435','97510','46040',
                          '33968','20256','42332','16480','54277',
                          '82819','93799','69101','57689','42821',
                          '68073','49342','46915','25825','92332',
                          '20219','96577','49463','19221','35814',
                          '64783','97303','52061','24357','58167',
                          '56286','64474','99847','53626','39703',
                          '24880','24365','50652','29611','90638',
                          '59246','27171','30483','11708','38630',
                          '81914','48269','11720','88662','68844',
                          '54838','93795','38102','33481','97546',
                          '49306','97216','49032','14270','72418',
                          '32540','53208','15588','29990','10407',
                          '92334','48543','51495','77996','53686',
                          '14827','30978','30482','86296','48869',
                          '59600','29495','24775','34645','19763',
                          '98602','20456','10468','13887','65714',
                          '74740','37096','96240','44111','54109',
                          '62693','87874','64295','62027','86027',
                          '54341','68582','67809','44159','97913',
                          '79150','38974','64754','73946','20840',
                          '16138','58939','20428','19890','70842',
                          '78648','55576','37267','40470','12957',
                          '57553','53593','34067','22555','79719',
                          '25809','28496','11083','87624','83622',
                          '84898','28678','14297','79461','22910',
                          '87129','49941','64817','93905','39721',
                          '81837','18753','86432','67821','66080',
                          '28246','13466','16363','56950','35446',
                          '58326','11760','33962','28399','45848',
                          '52560','66894','15169','20988','85925',
                          '38582','34825','94227','56758','24801',
                          '14128','14012','35824','49784')
     intersect
      select ca_zip
      from (SELECT substr(ca_zip,1,5) ca_zip,count(*) cnt
            FROM {{ source('tpcds_sf10tcl', 'customer_address') }} , {{ source('tpcds_sf10tcl', 'customer') }} 
            WHERE ca_address_sk = c_current_addr_sk and
                  c_preferred_cust_flag='Y'
            group by ca_zip
            having count(*) > 10)A1)A2) V1
 where ss_store_sk = s_store_sk
  and ss_sold_date_sk = d_date_sk
  and d_qoy = 1 and d_year = 2002
  and (substr(s_zip,1,2) = substr(V1.ca_zip,1,2))
 group by s_store_name
 order by s_store_name

