# Contributing to Quiry

Thanks for your interest in contributing. Quiry is a small project maintained by one person, so all types of contributions are very much appreciated.

## Ground rules

Be respectful and considerate of others in issues, pull requests, and reviews — disagree with the code, not the person. Hostile or abusive behavior gets a thread locked or a person permanently blocked. For private or sensitive concerns, contact me directly.

Before you start, fork the repository and clone it locally. If you're planning something beyond a small, self-contained fix, open an issue first and discuss what you have in mind.

## Reporting a bug

Open an issue with a minimal reproduction setup; the smallest snippet that shows the problem. Include your environment details, and which transport you're using, `child_process` or `worker_threads`, since a lot of bugs in a framework like this only show up on one of the two.

State the expected behavior, and what actually happened. If there's a stack trace involved, paste it in full rather than summarizing. The exact wording and status code usually points straight at the cause.

## Reporting a security issue

Quiry bridges two ends of a transport that are assumed to trust each other. An exposed object being callable by whatever's attached to that session is expected behavior, not a vulnerability on its own. Anything that consume unbounded memory or file descriptors through repeated calls, execute something outside the exposed objects' scope, or read data it was never handed a reference to, is a security issue.

Please don't open a public issue for these. Email me directly at <shizuka.yashiro@gmail.com> with the details and, if you have one, a reproduction. I'll acknowledge any report as soon as possible, and keep you updated as I work through it.

## Making a change

Keep pull requests small and focused. Larger changes usually means either the change is doing more than one thing, or it should have started as an issue first. If you find yourself fixing a bug and also making a feature change, please split it into two separate PRs.

If your change fixed a bug, include a regression test that fails before the fix and passes after it, using vitest like the rest of the suite. A regression test is critical for ensuring the fix doesn't regress or get undone by future changes.

> [Conventional commits](https://gist.github.com/qoomon/5dfcdf8eec66a051ecd85625518cfd13) style is enforced. Write commit messages that are clear and concise, describing the change being made, if it's not obvious, why.

## License

By contributing, you agree that your changes are licensed under the same [Apache License 2.0](./LICENSE) that covers the rest of the project.
