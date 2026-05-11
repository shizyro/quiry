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

type Primitive = string | number | boolean | bigint | null | undefined;
type Serializable = Primitive | readonly Serializable[] | { readonly [key: string]: Serializable };

type DeepRequired<T> = Required<{
  [K in keyof T]: T[K] extends Required<T[K]> ? T[K] : DeepRequired<T[K]>;
}>;

type Unsubscribe = () => void;
