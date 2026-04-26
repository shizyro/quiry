interface Logger {
  error: LeveledLogMethod;
  warn: LeveledLogMethod;
  info: LeveledLogMethod;
  debug: LeveledLogMethod;
  trace: LeveledLogMethod;
}

interface LeveledLogMethod {
  (message: string, ...meta: any[]): Logger;
  (message: any): Logger;
  (infoObject: object): Logger;
}
