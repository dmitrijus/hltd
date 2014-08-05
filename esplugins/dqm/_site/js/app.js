var dqmApp = angular.module('dqmApp', ['ngRoute']);

dqmApp.controller('LogController', ['$scope', 'ElasticQuery', function($scope, ElasticQuery) {
	var query = {
		"query": {
			"match_all": {}
	    },
		"fields" : ["_source", "_timestamp"],
		"size": 15,
		"sort": [
			{ "_timestamp": { "order": "desc" }}
		]
	};

	ElasticQuery.repeated_search("log", "_search/", query, function (body) {
		$scope.hits = body.hits.hits;
	});

	$scope.toJson = function (v) {
		return angular.toJson(v, true);
	};

    $scope.$on("$destroy", function handler() {
        // destruction code here
		ElasticQuery.delete_search("log");
    });
}]);

dqmApp.controller('NavigationController', ['$scope', '$location', '$route', '$http', function($scope, $location, $route, $http) {
	$scope.$route = $route;
	$scope.setPage = function (str) {
		$location.path("/" + str);
	};
	
	$scope.NavigationController = {};
	$scope.$watch(function () { return $http.pendingRequests.length; }, function (v) {
		$scope.NavigationController.http_count = v;
		$scope.NavigationController.http_state = v?"busy":"ready";
	});
}]);

dqmApp.factory('ElasticQuery', ['$http', '$window', function ($http, $window) {
	var factory = {
		base: "http://127.0.0.1:9200/dqm_online_monitoring/",
		_searches: {},
		_ti: null
	};

	factory.repeated_search = function (name, url, query, cb) {
		var desc = {
			real_url: this.base + url,
			url: url,
			query: query,
			callback: cb
		};

		this._searches[name] = desc;
		this.do_the_query();
	};

	factory.delete_search = function (name) {
		delete this._searches[name];
	};

	factory.do_the_query = function () {
		angular.forEach(this._searches, function(value, key) {
			var p = $http.post(value.real_url, value.query);
			p.success(value.callback);
     	});
	};

	factory.try_update = function () {
		console.log("Updating queries: ", this._searches);
		if ($http.pendingRequests.length == 0) {
			this.do_the_query();
		};
	};

	factory.start = function (timeout_sec) {
		if (this._ti) {
			$window.clearInterval(this._ti);
			this._ti = null;
		}

		this._ti = $window.setInterval(function () { factory.try_update(); }, timeout_sec * 1000);
	};


	factory.start(1);
	factory.start(2);
	factory.start(3);
	factory.start(4);
	factory.start(5);
	return factory;
}]);

dqmApp.directive('prettifySource', function ($window) {
	return {
		restrict: 'A',
		scope: { 'prettifySource': '@' },
		link: function (scope, elm, attrs) {
			scope.$watch('prettifySource', function (v) {
				var lang = attrs.lang || "javascript";
				var x = hljs.highlight(lang, v || "") .value;
				elm.html(x);
			});
		}
	};
});

dqmApp.config(function($routeProvider, $locationProvider) {
  $routeProvider
	.when('/log', { menu: 'log', templateUrl: 'templates/log.html' })
	.when('/lumi', { menu: 'lumi', templateUrl: 'templates/lumi.html' })
	.otherwise({ redirectTo: '/log' });

  // configure html5 to get links working on jsfiddle
  $locationProvider.html5Mode(false);
});
