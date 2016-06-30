define(function (require) {
  return function MapperService(Private, Promise, es, config, kbnIndex) {
    var _ = require('lodash');
    var moment = require('moment');

    var IndexPatternMissingIndices = require('ui/errors').IndexPatternMissingIndices;
    var transformMappingIntoFields = Private(require('ui/index_patterns/_transform_mapping_into_fields'));
    var calculateIndices = Private(require('ui/index_patterns/_calculate_indices'));
    var intervals = Private(require('ui/index_patterns/_intervals'));
    var patternToWildcard = Private(require('ui/index_patterns/_pattern_to_wildcard'));

    var LocalCache = Private(require('ui/index_patterns/_local_cache'));

    function Mapper() {

      // Save a reference to mapper
      var self = this;

      // proper-ish cache, keeps a clean copy of the object, only returns copies of it's copy
      var fieldCache = self.cache = new LocalCache();

      /**
       * Gets an object containing all fields with their mappings
       * @param {dataSource} dataSource
       * @param {boolean} skipIndexPatternCache - should we ping the index-pattern objects
       * @returns {Promise}
       * @async
       */
      self.getFieldsForIndexPattern = function (indexPattern, skipIndexPatternCache) {
        var id = indexPattern.id;

        var cache = fieldCache.get(id);
        if (cache) return Promise.resolve(cache);

        if (!skipIndexPatternCache) {
          return es.get({
            index: kbnIndex,
            type: 'index-pattern',
            id: id,
            _sourceInclude: ['fields']
          })
          .then(function (resp) {
            if (resp.found && resp._source.fields) {
              fieldCache.set(id, JSON.parse(resp._source.fields));
            }
            return self.getFieldsForIndexPattern(indexPattern, true);
          });
        }

        var promise = self.getIndicesForNonIntervalIndexPattern(indexPattern);
        if (indexPattern.intervalName) {
          promise = self.getIndicesForIndexPattern(indexPattern)
          .then(function (existing) {
            if (existing.matches.length === 0) throw new IndexPatternMissingIndices();
            return existing.matches.slice(-config.get('indexPattern:fieldMapping:lookBack')); // Grab the most recent
          });
        }

        return promise.then(function (indexList) {
          return es.indices.getFieldMapping({
            index: indexList,
            field: '*',
            ignoreUnavailable: _.isArray(indexList),
            allowNoIndices: false,
            includeDefaults: true
          });
        })
        .catch(handleMissingIndexPattern)
        .then(transformMappingIntoFields)
        .then(function (fields) {
          fieldCache.set(id, fields);
          return fieldCache.get(id);
        });
      };

      self.getIndicesForIndexPattern = function (indexPattern) {
        return getAliases(patternToWildcard(indexPattern.id))
        .then(function (all) {
          var matches = all.filter(function (existingIndex) {
            var parsed = moment(existingIndex, indexPattern.id);
            return existingIndex === parsed.format(indexPattern.id);
          });

          return {
            all: all,
            matches: matches
          };
        })
        .catch(handleMissingIndexPattern);
      };

      self.getIndicesForNonIntervalIndexPattern = function (indexPattern) {
        return calculateIndices(indexPattern.id, indexPattern.timeFieldName, moment().subtract(30, 'days'), moment(), false)
        .then(function (indexList) {
          // Concatenate the indices if any. Return the index pattern in case no indeces are found.
          return _.map(indexList, 'index').join(',') || indexPattern.id;
        })
        .then(function (index) {
          return getAliases(index)
          .catch(handleMissingIndexPattern);
        });
      };

      /**
       * Clears mapping caches from elasticsearch and from local object
       * @param {dataSource} dataSource
       * @returns {Promise}
       * @async
       */
      self.clearCache = function (indexPattern) {
        fieldCache.clear(indexPattern);
        return Promise.resolve();
      };
    }

    function getAliases(indexList) {
      return es.indices.getAliases({
        index: indexList
      })
      .then(function (resp) {
        return _(resp)
        .map(function (index, key) {
          if (index.aliases) {
            return [Object.keys(index.aliases), key];
          } else {
            return key;
          }
        })
        .flattenDeep()
        .sort()
        .uniq(true)
        .value();
      });
    };

    function handleMissingIndexPattern(err) {
      if (err.status >= 400) {
        // transform specific error type
        return Promise.reject(new IndexPatternMissingIndices());
      } else {
        // rethrow all others
        throw err;
      }
    }

    return new Mapper();
  };
});
