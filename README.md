# join-two-scd-type2-tables
it is one of the common problem in data-warehousing to join two scd type-2 tables.


# what is scd-type2
when ever there is a change in record we wont update the record, insted we create new record by making the previous one inactive and newly created record being the current active record

## for example:
consider a student table
  
![image](https://user-images.githubusercontent.com/36265552/136322797-dafbde21-ca03-4906-8491-d3f39f9eeef6.png)

if there is change in any record then we create a new record and make the old record incactive by ading end-date to it and also making isActive false

![image](https://user-images.githubusercontent.com/36265552/136323054-db617b33-baf6-412c-95eb-7d795b1eae86.png)


# joining two scd type 2 tables


