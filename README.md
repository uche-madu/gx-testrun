

### Steps to login to local postgres from my ubuntu terminal
1. Run and enter system password at the prompt:
    ```
    sudo systemctl start postgresql.service
    ```

Once you've started the PostgreSQL service using `systemctl`, you can proceed with the following steps to access and manage your PostgreSQL databases locally:

2. Access the PostgreSQL Command Line:
    By default, PostgreSQL comes with a user named postgres. You can switch to this user to access the PostgreSQL command line through the `psql` client:
    ```
    sudo -u postgres psql
    ```

3. Create a New Database:
    From the `psql` interface, you can create a new database using the `CREATE DATABASE` command. For example, to create a database named `expectations_db`:
    ```sql
    CREATE DATABASE expectations_db;
    ```

4. Create a New User:
    You can create a new user (also called a role) using the `CREATE ROLE` command. For example, to create a user named `admin` with a password `secr3tpa55w0rd`:
    ```sql
    CREATE ROLE admin WITH LOGIN PASSWORD 'secr3tpa55w0rd';
    ```

5. Grant Privileges:
    To grant the necessary privileges to the user for the database and public schema:
        ```sql
        GRANT ALL PRIVILEGES ON DATABASE expectations_db TO admin;
        ```
6. Create Schema:
    ```sql
    CREATE SCHEMA data_pipelines AUTHORIZATION admin;
    ```

6. Exit psql:
    You can exit the psql interface with:
    ```sql
    \q
    ```

7. Access PostgreSQL with the New User:
    To access the new database with the newly created user:
    ```
    psql -U admin -d expectations_db -h localhost
    ```
    You'll be prompted for the password you set for `admin` which is `secr3tpa55w0rd`.

    `Note:` To avoid the `Peer authentication failed for user "admin"` error,which occurs because by default, PostgreSQL uses "peer" authentication for local connections, meaning that it checks if the OS username matches the PostgreSQL username, do the following:

    * Modify the Authentication Method:
        - Open the `pg_hba.conf` file. This file controls the client authentication. Its location might vary depending on your installation, but it's commonly found in `/etc/postgresql/{version}/main/`.
            ```
            sudo nano /etc/postgresql/{version}/main/pg_hba.conf
            ```
        - Replace `{version}` with your PostgreSQL version, e.g., `12` or `15`.

        - Find the lines that look like:
            ```sql
            local   all             all                                     peer
            ```
        - And change peer to md5:
            ```sql
            local   all             all                                     md5
            ```
        - Save and close the file.

        - Restart the PostgreSQL service and try to access PostgreSQL again with the new user:
            ```
            sudo systemctl restart postgresql
            ```