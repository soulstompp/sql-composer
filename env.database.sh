# source this file into your shell to set ENV variables after running docker-compose up:
#     . ./env.database.sh
export MYSQL_DATABASE_URL=mysql://runner@$(docker port sql_composer_mysql 3306)/sql_composer
export PG_DATABASE_URL=postgresql://runner@$(docker port sql_composer_postgres 5432)/sql_composer
