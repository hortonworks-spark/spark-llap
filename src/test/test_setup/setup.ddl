
!connect jdbc:hive2://localhost:10000 hive password

DROP TABLE IF EXISTS employee;

CREATE EXTERNAL TABLE employee(employee_id INT,full_name VARCHAR(30),first_name VARCHAR(30),last_name VARCHAR(30),position_id INT,position_title VARCHAR(30),store_id INT,department_id INT,birth_date DATE,hire_date TIMESTAMP,end_date TIMESTAMP,salary DECIMAL(10,4),supervisor_id INT,education_level VARCHAR(30),marital_status VARCHAR(30),gender VARCHAR(30),management_role VARCHAR(30))
row format delimited fields terminated by '\011'
;
load data local inpath '${local_path}/employee' overwrite into table employee;

DROP TABLE IF EXISTS salary;
CREATE EXTERNAL TABLE salary(pay_date TIMESTAMP,employee_id INT,department_id INT,currency_id INT,salary_paid DECIMAL(10,4),overtime_paid DECIMAL(10,4),vacation_accrued DOUBLE,vacation_used DOUBLE)
row format delimited fields terminated by '\011'
;
load data local inpath '${local_path}/salary' overwrite into table salary;

!quit
