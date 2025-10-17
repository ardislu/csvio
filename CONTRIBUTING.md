# Contributing

Checklist for changes and releases.

## 1. Type checker

Run the type checker:

```
npm run types:check
```

All reasonable type issues (i.e., those not resulting from limitations of `tsc`) should be fixed.

To work around `tsc` limitations, add manual type definitions in `./npm/ambient.d.ts`. These type definitions are appended to the output of `tsc` and override the `tsc` outputs if there are conflicts.

## 2. Tests

Run tests:

```
npm run test
npm run test:large
```

All tests should pass.

Check code coverage:

```
npm run test:coverage
```

Coverage should be maintained at 100%.

## 3. Benchmarks

Run benchmarks before and after changes to check for performance regressions:

```
npm run bench
```

Performance regressions should be fixed or explained.

## 4. Debugging code

Search for and delete code that was written for debugging purposes:
- `debugger`
- `only: true` (in tests)
- `console`
- ...etc.

No debugging code should be committed.

## 5. (Release only) Dev package

Create a dev package to test the latest changes:

```
npm run pack:dev
```

A dev package is identical to a production package created by `npm pack`, except:
- `version` is set to `0.0.0-DEV-[timestamp]`.
- `private` is set to `true` to prevent accidental publishing.

Move the `.tgz` package to a new Node.js project and then install the package:

```
npm i -D ./ardislu-csvio-0.0.0-DEV-NNNNNNNNNNNNN.tgz
```

Manually review for issues (i.e., try integrating the library in the Node.js project).

## 6. (Release only) `package.json` and `package-lock.json`

Update the `version` key in `package.json`.

Bump dependencies in:
- `package.json`
- `release.yml`
- `.node-version`

Do `npm install` to update `package-lock.json`.

## 7. (Release only) Release tag

Cut a new release by adding a new annotated tag.

Use the "v" prefix for versions (i.e., the correct tag is **"v1.0.0"**, NOT "1.0.0").

Put release notes in the tag message:
- The first line is the subject line and should always be "Release $VERSION".
- The body should be a human-friendly summary of important changes (see [`esbuild`](https://github.com/evanw/esbuild/releases) release notes for reference).
- Use Markdown for the body.

Example:

```
$VERSION="v1.0.0"
git tag -a $VERSION -m "Release $VERSION

Example release note description:
- Example point 1
- Example point 2
- Example point 3

Example sentence with [a link](https://example.com)."
```

For long release notes, it may be easier to save the notes to a temporary file and pass the file:

```
$VERSION="v1.0.0"
git tag -a $VERSION -F m.txt
```

To check the release notes for a version:

```
$VERSION="v1.0.0"
git for-each-ref --format="%(contents:subject)%0a%0a%(contents:body)" "refs/tags/$VERSION"
```

Previous tags and release notes can be viewed in [GitHub](https://github.com/ardislu/csvio/tags), [Codeberg](https://codeberg.org/ardislu/csvio/tags), or [git.ardis.lu](https://git.ardis.lu/?p=csvio;a=tags).
