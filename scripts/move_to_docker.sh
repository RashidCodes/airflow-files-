# This script is used to copy the employees.csv file to the docker container 

docker cp employees.csv datawarehouse:/var/opt/mssql/


if [[ $? -ne 0 ]]
then 
  echo "Unable to copy employees.csv to the datawarehouse" >&2
  exit 1
fi

echo "Successfully copied employees.csv to the datawarehouse. Ready for BULK INSERTION"

exit 0
