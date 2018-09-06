Requirements
-------------------------------------
1. JDK 8 suitable for your platform.
2. Supported browsers: Chrome, Firefox, Safari, Edge.
3. Ignite cluster should be started with `ignite-rest-http` module in classpath.
 For this copy `ignite-rest-http` folder from `libs\optional` to `libs` folder.

How to run
-------------------------------------
1. Unpack ignite-web-console-x.x.x.zip to some folder.
2. Change work directory to folder where Web Console was unpacked.
3. Start ignite-web-console-xxx executable for you platform:
    For Linux: `sudo ./ignite-web-console-linux`
    For macOS: `sudo ./ignite-web-console-macos`
    For Windows: `ignite-web-console-win.exe`

NOTE: For Linux and macOS `sudo` required because non-privileged users are not allowed to bind to privileged ports (port numbers below 1024).

4. Open URL `localhost` in browser.
5. Login with user `admin@admin` and password `admin`.
6. Start web agent from folder `web agent`. For Web Agent settings see `web-agent\README.txt`.

NOTE: Cluster URL should be specified in `web-agent\default.properties` in `node-uri` parameter.

Technical details
-------------------------------------
1. Package content:
    `libs` - this folder contains Web Console and MongoDB binaries.
    `user_data` - this folder contains all Web Console data (registered users, created objects, ...) and
     should be preserved in case of update to new version.
    `web-agent` - this folder contains Web Agent.
2. Package already contains MongoDB for macOS, Windows, RHEL, CentOs and Ubuntu on other platforms MongoDB will be downloaded on first start. MongoDB executables will be downloaded to `libs\mogodb` folder.
3. Web console will start on default HTTP port `80` and bind to all interfaces `0.0.0.0`.
3. To bind Web Console to specific network interface:
    On Linux: `./ignite-web-console-linux --server:host 192.168.0.1`
    On macOS: `sudo ./ignite-web-console-macos --server:host 192.168.0.1`
    On Windows: `ignite-web-console-win.exe --server:host 192.168.0.1`
4. To start Web Console on another port, for example `3000`:
    On Linux: `sudo ./ignite-web-console-linux --server:port 3000`
    On macOS: `./ignite-web-console-macos --server:port 3000`
    On Windows: `ignite-web-console-win.exe --server:port 3000`

All available parameters with defaults:
    Web Console host:           --server:host 0.0.0.0
    Web Console port:           --server:port 80
    Enable HTTPS:               --server:ssl false
    HTTPS key:                  --server:key "serve/keys/test.key"
    HTTPS certificate:          --server:cert "serve/keys/test.crt"
    HTTPS passphrase:           --server:keyPassphrase "password"
    MongoDB URL:                --mongodb:url mongodb://localhost/console
    Mail service:               --mail:service "gmail"
    Signature text:             --mail:sign "Kind regards, Apache Ignite Team"
    Greeting text:              --mail:greeting "Apache Ignite Web Console"
    Mail FROM:                  --mail:from "Apache Ignite Web Console <someusername@somecompany.somedomain>"
    User to send e-mail:        --mail:auth:user "someusername@somecompany.somedomain"
    E-mail service password:    --mail:auth:pass ""

Sample usage:
    `ignite-web-console-win.exe --mail:auth:user "my_user@gmail.com"  --mail:auth:pass "my_password"`

Advanced configuration of SMTP for Web Console.
-------------------------------------
1. Create sub-folder "config" in folder with Web Console executable.
2. Create in config folder file "settings.json".
3. Specify SMTP settings in settings.json (updating to your specific names and passwords):

Sample "settings.json":
{
    "mail": {
        "service": "gmail",
        "greeting": "My Company Greeting",
        "from": "My Company Web Console <some_name@gmail.com>",
        "sign": "Kind regards,<br>My Company Team",
        "auth": {
            "user": "some_name@gmail.com",
            "pass": "my_password"
        }
    }
}

Web Console sends e-mails with help of NodeMailer: https://nodemailer.com.

Documentation available here:
   https://nodemailer.com/smtp
   https://nodemailer.com/smtp/well-known

In case of non GMail SMTP server it may require to change options in "settings.json" according to NodeMailer documentation.

Troubleshooting
-------------------------------------
1. On Windows check that MongoDB is not blocked by Antivirus/Firewall/Smartscreen.
2. Root permission is required to bind to 80 port under Mac OS X and Linux, but you may always start Web Console on another port if you don't have such permission.
3. For extended debug output start Web Console as following:
	On Linux execute command in terminal: `DEBUG=mongodb-* ./ignite-web-console-linux`
	On Windows execute two commands in terminal:
		`SET DEBUG=mongodb-*`
		`ignite-web-console-win.exe`
