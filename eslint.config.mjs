// ESLint flat config for spx-backtester. The pre-commit hook
// scripts/check-html-js.sh extracts inline <script> blocks from .html
// files into temp .js files and runs ESLint on them with this config.
//
// PURPOSE: catch the class of bugs that bit on 2026-05-29 — `const`/`let`
// declared inside an if-block, referenced outside the block. ReferenceError
// at render time, blanks the page. Should never reach production again.
//
// The no-undef rule catches this. Browser env covers DOM/Intl/fetch/etc.
// `globals` lists project-specific names that ESLint shouldn't flag.

export default [
  {
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: {
        // Browser DOM + standard runtime
        document: "readonly", window: "readonly", location: "readonly",
        navigator: "readonly", localStorage: "readonly", sessionStorage: "readonly",
        fetch: "readonly", Request: "readonly", Response: "readonly", Headers: "readonly",
        URL: "readonly", URLSearchParams: "readonly", FormData: "readonly",
        Blob: "readonly", File: "readonly", FileReader: "readonly",
        setTimeout: "readonly", clearTimeout: "readonly",
        setInterval: "readonly", clearInterval: "readonly",
        requestAnimationFrame: "readonly", cancelAnimationFrame: "readonly",
        console: "readonly", alert: "readonly", confirm: "readonly", prompt: "readonly",
        addEventListener: "readonly", removeEventListener: "readonly",
        Intl: "readonly", Math: "readonly", JSON: "readonly", Date: "readonly",
        Object: "readonly", Array: "readonly", Number: "readonly", String: "readonly",
        Boolean: "readonly", Map: "readonly", Set: "readonly", WeakMap: "readonly",
        Promise: "readonly", Symbol: "readonly", Error: "readonly", TypeError: "readonly",
        ReferenceError: "readonly", RangeError: "readonly", SyntaxError: "readonly",
        Reflect: "readonly", Proxy: "readonly", parseInt: "readonly", parseFloat: "readonly",
        isNaN: "readonly", isFinite: "readonly", NaN: "readonly", Infinity: "readonly",
        undefined: "readonly", globalThis: "readonly",
        Uint8Array: "readonly", Uint16Array: "readonly", Uint32Array: "readonly",
        Int8Array: "readonly", Int16Array: "readonly", Int32Array: "readonly",
        Float32Array: "readonly", Float64Array: "readonly", ArrayBuffer: "readonly",
        DataView: "readonly", TextEncoder: "readonly", TextDecoder: "readonly",
        crypto: "readonly", performance: "readonly", AbortController: "readonly",
        AbortSignal: "readonly", Event: "readonly", CustomEvent: "readonly",
        EventTarget: "readonly", MutationObserver: "readonly", ResizeObserver: "readonly",
        IntersectionObserver: "readonly",
        getComputedStyle: "readonly", DecompressionStream: "readonly",
        CompressionStream: "readonly", ReadableStream: "readonly", WritableStream: "readonly",
        TransformStream: "readonly", Worker: "readonly", SharedWorker: "readonly",
        // Common third-party libs (loaded via <script src=...> CDN)
        Plotly: "readonly", Chart: "readonly", d3: "readonly",
        html2canvas: "readonly", jsPDF: "readonly", jspdf: "readonly",
        // Project globals — attached to globalThis by signal-engine.js
        SignalEngine: "readonly",
        // Legacy backtester.html uses `window.X = ...` pattern with cross-block
        // implicit-global reads. Adding here so the lint passes without
        // refactoring an older page that already works at runtime.
        SPECIAL_DATE_SETS: "writable",
        lastResult: "writable",
      },
    },
    rules: {
      // THE rule that catches the 2026-05-29 stradEffSpot bug:
      // a `const` declared inside an if-block, referenced outside the block
      // → ReferenceError at runtime, blanks the page. ESLint flags this as
      // no-undef because the reference outside the block has no declaration
      // in scope.
      "no-undef": "error",

      // Catches the dual of the above — var declared in nested block
      // hoisted to function scope (subtle bug source).
      "block-scoped-var": "error",

      // Duplicate declarations are always a bug.
      "no-redeclare": "error",

      // Use === / !== — silent type coercion has bitten this codebase before.
      // Warning only because there are legitimate use cases (== null guards).
      "eqeqeq": ["warn", "smart"],

      // Don't suppress shadowing in nested scopes — explicit > implicit.
      "no-shadow-restricted-names": "error",
    },
  },
];
