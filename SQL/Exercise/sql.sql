/* Given Employee Table with two columns ID, Salary 10, 2000 11, 5000 12, 3000 */
DROP TABLE IF EXISTS Employee;
CREATE TABLE IF NOT EXISTS Employee (ID INT PRIMARY KEY, Salary number);
INSERT INTO Employee (ID, Salary) values (10, 2000);
INSERT INTO Employee (ID, Salary) values (11, 5000);
INSERT INTO Employee (ID, Salary) values (12, 3000);

/* Write sql query to get the second highest salary among all employees? */
SELECT ID, MAX(Salary) FROM Employee WHERE Salary not in (SELECT MAX(Salary) FROM Employee);

/* Given Employee table with three columns ID, Salary, DeptID 10, 1000, 2 20, 5000, 3 30, 3000, 2 */
DROP TABLE IF EXISTS Employee;
CREATE TABLE IF NOT EXISTS Employee (ID INT PRIMARY KEY, Salary number, DeptID INT);
INSERT INTO Employee (ID, Salary, DeptID) values (10, 1000, 2);
INSERT INTO Employee (ID, Salary, DeptID) values (20, 5000, 3);
INSERT INTO Employee (ID, Salary, DeptID) values (30, 3000, 2);
DROP TABLE IF EXISTS Department;
CREATE TABLE IF NOT EXISTS Department (ID INT PRIMARY KEY, DeptName VARCHAR(50));
INSERT INTO Department (ID, DeptName) values (1, 'Marketing');
INSERT INTO Department (ID, DeptName) values (2, 'IT');
INSERT INTO Department (ID, DeptName) values (3, 'Finance');

/* Write sql query to find max salary and department name from each department. */
SELECT MAX(e.salary) AS max_salary, d.DeptName FROM Department d LEFT OUTER JOIN Employee e ON e.DeptID = d.ID GROUP BY d.DeptName;

/* Employee table: ID NAME EMAIL 10 John jbaldwin 20 George gadams 30 John jsmith */
DROP TABLE IF EXISTS Employee;
CREATE TABLE Employee (ID INT PRIMARY KEY, Name VARCHAR(30), Email VARCHAR(30));
INSERT INTO Employee (ID, Name, Email) values (10, 'John', 'jbaldwin');
INSERT INTO Employee (ID, Name, Email) values (20, 'George', 'gadams');
INSERT INTO Employee (ID, Name, Email) values (30, 'John', 'jsmith');

/* Write sql query to find employees that have same name or email.*/
SELECT e1.ID, e1.Name, e1.Email FROM Employee e1 JOIN Employee e2 WHERE e1.ID != e2.ID AND (e1.Email = e2.Email OR e1.Name = e2.Name);

/* Given Employee table with three columns ID, Salary, DeptID 10, 1000, 2 20, 5000, 3 30, 3000, 2 */
DROP TABLE IF EXISTS Employee;
CREATE TABLE IF NOT EXISTS Employee (ID INT PRIMARY KEY, Salary number, DeptID INT);
INSERT INTO Employee (ID, Salary, DeptID) values (10, 1000, 2);
INSERT INTO Employee (ID, Salary, DeptID) values (20, 5000, 3);
INSERT INTO Employee (ID, Salary, DeptID) values (30, 3000, 2);

/* Write sql query to find max salary from each department */
SELECT MAX(Salary) AS max_salary, DeptID FROM Employee GROUP BY DeptID;

/* Given Employee Table with two columns ID, Salary 10, 2000 11, 5000 12, 3000 */
DROP TABLE IF EXISTS Employee;
CREATE TABLE Employee (ID INT PRIMARY KEY, Salary NUMBER);
INSERT INTO Employee (ID, Salary) VALUES (10, 2000);
INSERT INTO Employee (ID, Salary) VALUES (11, 5000);
INSERT INTO Employee (ID, Salary) VALUES (12, 3000);

/* How can you find 10 employees with odd number as employee id? */
SELECT * FROM Employee WHERE ID % 2 = 1 LIMIT 10;

/* Employee table: ID NAME EMAIL 10 John jsmith 20 George gadams 30 Jane jsmith */ 
DROP TABLE IF EXISTS Employee;
CREATE TABLE Employee (ID INT PRIMARY KEY, Name VARCHAR(30), Email VARCHAR(30));
INSERT INTO Employee (ID, Name, Email) values (10, 'John', 'jsmith');
INSERT INTO Employee (ID, Name, Email) values (20, 'George', 'gadams');
INSERT INTO Employee (ID, Name, Email) values (30, 'Jane', 'jsmith');
INSERT INTO Employee (ID, Name, Email) values (40, 'Richard', 'richards');

/* Write query to find employees with duplicate email. */
SELECT Name FROM Employee WHERE Email IN (SELECT Email FROM Employee GROUP BY Email HAVING COUNT(*) > 1);

/* Write a query to find all employee whose name contains the word "rich", regardless of case. */
SELECT * FROM Employee WHERE LOWER(Name) like '%rich%';