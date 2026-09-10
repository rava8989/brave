// ESLint flat config for the schedule app (classic browser scripts, no build).
// Run:  npx eslint --config eslint.config.mjs js worker tests
export default [
  {
    files: ['js/**/*.js'],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'script',
      globals: {
        window: 'readonly', document: 'readonly', location: 'readonly', navigator: 'readonly',
        localStorage: 'readonly', indexedDB: 'readonly', fetch: 'readonly', FileReader: 'readonly', Image: 'readonly',
        Blob: 'readonly', URL: 'readonly', setTimeout: 'readonly', clearTimeout: 'readonly', setInterval: 'readonly', clearInterval: 'readonly',
        console: 'readonly', module: 'readonly', require: 'readonly', Promise: 'readonly',
        // project globals (one per file, in load order)
        APP_CONFIG: 'readonly', SCHEDULE_CONFIG: 'readonly', ROSTER: 'readonly',
        Engine: 'readonly', Store: 'readonly', Swaps: 'readonly', Views: 'readonly', UI: 'readonly', Forms: 'readonly', App: 'readonly',
      },
    },
    rules: { 'no-undef': 'error', 'no-redeclare': 'off', 'block-scoped-var': 'error', 'no-unused-vars': ['warn', { args: 'none' }], eqeqeq: ['warn', 'smart'] },
  },
  {
    files: ['worker/**/*.js'],
    languageOptions: { ecmaVersion: 2022, sourceType: 'module', globals: { fetch: 'readonly', Response: 'readonly', Request: 'readonly', Headers: 'readonly', URL: 'readonly', console: 'readonly', crypto: 'readonly', atob: 'readonly', btoa: 'readonly', TextEncoder: 'readonly' } },
    rules: { 'no-undef': 'error', 'no-unused-vars': ['warn', { args: 'none' }] },
  },
  {
    files: ['tests/**/*.js'],
    languageOptions: { ecmaVersion: 2022, sourceType: 'script', globals: { require: 'readonly', module: 'readonly', __dirname: 'readonly', process: 'readonly', console: 'readonly' } },
    rules: { 'no-undef': 'error' },
  },
];
