type Primitive = string | number | boolean | bigint | null | undefined;
type Serializable = Primitive | readonly Serializable[] | { readonly [key: string]: Serializable };

type Unsubscribe = () => void;
