<!DOCTYPE html>
<html lang="en" ng-app="docToolApp">
    <head>
        <title>Documentation Tool</title>
        <script>window.user_email = "${user_email}"</script>
        <script src="http://ajax.googleapis.com/ajax/libs/angularjs/1.3.14/angular.min.js"></script>
        <script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.3.14/angular-route.min.js"></script>
        <script src="//maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>
        <link rel="stylesheet" type="text/css" href="//maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css"/>

        <script src="/static/js/app.js"></script>
        <script src="/static/js/navbar.js"></script>
        <script src="/static/js/home.js"></script>
        <script src="/static/js/table-view.js"></script>

        <link rel="stylesheet" type="text/css" href="/static/css/schematizer.css"/>
    </head>

    <body>

        <!-- Navbar -->
        <nav class="navbar navbar-default navbar-static-top" role="navigation" ng-controller="NavbarController">
            <div class="container">
                <div class="navbar-header">
                    <button type="button" class="navbar-toggle" data-toggle="collapse" data-target="doc-tool-navbar">
                        <span class="sr-only">Toggle navigation</span>
                        <span class="icon-bar"></span>
                    </button>
                    <span class="navbar-brand"><a href="#/home">Watson</a></span>
                </div>
                <div class="collapse navbar-collapse" id="doc-tool-navbar">
                    <ul class="nav navbar-nav navbar-right">
                        <li ng-class="{ active: isActive('/about')}" class="pull-right"><a href="#/about">About</a></li>
                    </ul>
                </div>
            </div>
        </nav>

        <div ng-view></div>
    </body>
</html>
