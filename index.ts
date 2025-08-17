type Result<Val, Err> = { ok: true; value: Val; } | { ok: false; error: Err; };

const Result = Object.freeze({
  Ok<T, E>(value: T): Result<T, E> {
    return { ok: true, value };
  },
  Err<T, E>(error: E): Result<T, E> {
    return { ok: false, error };
  },
});

type NativeFieldKind = 'int' | 'blob';
type TransformedFieldKind =
  | 'json'
  | 'utf8'
  // | 'flt32'
  | 'bifmt'
  ;

type BiFmtNativeField<Kind extends NativeFieldKind = NativeFieldKind> = {
  [K in NativeFieldKind]: K extends 'int'
  ? { kind: 'int'; name: string; value: number; }
  : K extends 'blob'
  ? { kind: 'blob'; name: string; value: Uint8Array; }
  : { kind: `Unsupported native field kind ${Kind}`, name: string; };
}[Kind];

type BiFmtTransformedField<Kind extends TransformedFieldKind = TransformedFieldKind> = {
  [K in TransformedFieldKind]: K extends 'json'
  ? { kind: 'json'; name: string; value: unknown; buffer: Uint8Array; }
  : K extends 'utf8'
  ? { kind: 'utf8'; name: string; value: string; buffer: Uint8Array; }
  // : K extends 'flt32'
  // ? { kind: 'flt32'; name: string; value: number; buffer: Uint8Array; }
  : { kind: `Unsupported transformed field kind ${Kind}`, name: string; buffer: Uint8Array; };
}[Kind];

type BiFmtField =
  | BiFmtNativeField
  | BiFmtTransformedField
  ;

type BiFmtFieldKind = NativeFieldKind | TransformedFieldKind;

// @ts-expect-error Kind can index the object I've created, trust me bro
type PickBiFmtField<Kind extends BiFmtFieldKind> = {
  [F in BiFmtField as F extends { value: any } ? F['kind'] : never]: F;
}[Kind];

interface BiFmtStruct {
  [field: string]: BiFmtField | [BiFmtField, BiFmtField, ...BiFmtField[]];
}

type Bytes = Uint8Array | Uint8ClampedArray | number[];

const FIELD_START_BYTE = 58; // Colon (':')

const FIELD_DATA_SEPARATOR_BYTE = 10; // NewLine
const FIELD_PIECE_SEPARATOR_BYTE = 32; // Space

// Printable ASCII table goes from 32 (Space) till 127 (Delete) but delete is a weird character
// so we ignore that specific character for our case.
const MIN_PRINTABLE_ASCII_TABLE_BYTE = 32;
const MAX_PRINTABLE_ASCII_TABLE_BYTE = 126;

const FIELD_INT_LITERAL_0_BYTE = 48; // ascii 0
const FIELD_INT_LITERAL_1_BYTE = 49; // ascii 1
const FIELD_INT_LITERAL_2_BYTE = 50; // ascii 2
const FIELD_INT_LITERAL_3_BYTE = 51; // ascii 3
const FIELD_INT_LITERAL_4_BYTE = 52; // ascii 4
const FIELD_INT_LITERAL_5_BYTE = 53; // ascii 5
const FIELD_INT_LITERAL_6_BYTE = 54; // ascii 6
const FIELD_INT_LITERAL_7_BYTE = 55; // ascii 7
const FIELD_INT_LITERAL_8_BYTE = 56; // ascii 8
const FIELD_INT_LITERAL_9_BYTE = 57; // ascii 9

const FIELD_INT_LITERAL_BYTES = Object.freeze([
  FIELD_INT_LITERAL_0_BYTE,
  FIELD_INT_LITERAL_1_BYTE,
  FIELD_INT_LITERAL_2_BYTE,
  FIELD_INT_LITERAL_3_BYTE,
  FIELD_INT_LITERAL_4_BYTE,
  FIELD_INT_LITERAL_5_BYTE,
  FIELD_INT_LITERAL_6_BYTE,
  FIELD_INT_LITERAL_7_BYTE,
  FIELD_INT_LITERAL_8_BYTE,
  FIELD_INT_LITERAL_9_BYTE,
] as const);

const FIELD_INT_KIND_BYTE = 105; // Lowercase letter 'i'
const FIELD_BLOB_KIND_BYTE = 98; // Lowercase letter 'b'

const FIELD_KIND_BYTES = Object.freeze([
  FIELD_INT_KIND_BYTE,
  FIELD_BLOB_KIND_BYTE,
] as const);

// Just a simple tagging class
export class ParseBiFmtFieldError extends Error { }

export class ParseBiFmtFieldUnexpectedByteError extends ParseBiFmtFieldError {
  constructor(expected: number, received: number) {
    super(`Bi format field expected byte ${expected} but got ${received}`);
  }
}

export class ParseBiFmtFieldUnexpectedEOFError extends ParseBiFmtFieldError {
  constructor(expected: number = -1) {
    if (expected == -1) {
      super(`Bi format field unexpectedly hit the end of bytes list when reading field information`);
    } else {
      super(`Bi format field expected byte ${expected} but hit the end of bytes list`);
    }
  }
}

export class ParseBiFmtFieldInvalidKindError extends ParseBiFmtFieldError {
  constructor(found: number) {
    const sup = FIELD_KIND_BYTES.join(', ');
    super(`Invalid field kind was found in BiFmt, requested unsupported field kind '${found}' but the supported kinds are ${sup}`);
  }
}

export class ParseBiFmtFieldMissingNameError extends ParseBiFmtFieldError {
  constructor() {
    super('Bi Format field is missing a name. Name must be at least one printable ASCII character');
  }
}

export interface BiFmtReader {
  next: () => number | undefined;
  readonly cursor: number;
  peek_slice: (length: number) => Bytes;
  adv_slice: (length: number) => Bytes;

  on_byte: (byte: number) => boolean;
  on_eof: () => boolean;
}

function concat_u8_to_bytes<Base extends Uint8Array<any>>(base: Base, ...others: [Bytes, ...Bytes[]]): Uint8Array<ArrayBuffer> {
  let result = Uint8Array.from(base);
  for (const othr of others) {
    result = Uint8Array.from(new Proxy({ length: result.length + othr.length }, {
      get(t, p, r) {
        if (p === 'length') return t.length;
        if (typeof p === 'symbol') return Reflect.get(t, p, r);
        try {
          let idx = Number(p);
          if (!Number.isSafeInteger(idx)) return undefined;

          if (idx < result.length) {
            return result[idx];
          }

          idx -= result.length;
          if (idx < othr.length) {
            return result[idx];
          }
        } catch { }
        return undefined;
      }
    }));
  }
  return result;
}

class BiFormatReader implements BiFmtReader {
  #offset: number;
  #bytes: Uint8Array;
  #pointer_stack: number[];

  constructor(bytes: Bytes, offset = 0) {
    this.#bytes = Uint8Array.from(bytes);
    this.#offset = offset;
    this.#pointer_stack = [];
  }

  append_bytes(othr: Bytes): void;
  append_bytes(othr: Bytes, ...extras: Bytes[]): void;
  append_bytes(...extras: [Bytes, ...Bytes[]]): void {
    this.#bytes = concat_u8_to_bytes(this.#bytes, ...extras);
  }

  drop_bytes(n: number = this.#offset): void {
    const off = this.#offset;
    if (n > off) {
      console.warn('[BiFormatReader] Attempting to drop more bytes than read!');
    }
    if (n > this.#bytes.length) {
      console.error('[BiFormatReader] Attempting to drop more bytes than buffered!');
    }
    const count = Math.max(this.#bytes.length - n, 0);
    const bytes = new Uint8Array(count);
    const prevb = this.#bytes;
    for (let i = 0; i < bytes.length; ++i) {
      bytes[i] = prevb[off + i] ?? 0;
    }
    this.#bytes = bytes;
    this.#offset = Math.max(0, off - n);
  }

  get bytes_length() {
    return this.#bytes.length;
  }

  save_point(): void {
    this.#pointer_stack.push(this.#offset);
  }

  restore_point(): void {
    const pointer = this.#pointer_stack.pop();
    if (pointer == null) return;
    this.#offset = pointer;
  }

  drop_save_point(): void {
    this.#pointer_stack.pop();
  }

  next() {
    return this.#bytes[++this.#offset];
  }

  get cursor() {
    return this.#bytes[this.#offset] ?? -1;
  }

  peek_slice(n: number) {
    return this.#bytes.slice(this.#offset, this.#offset + n);
  }

  adv_slice(n: number) {
    const base = this.#offset;
    this.#offset += n;
    return this.#bytes.slice(base, base + n);
  }

  on_byte(byte: number) {
    return this.cursor === byte;
  }

  on_eof() {
    return this.#offset >= this.#bytes.length;
  }

  read_field(): BiFmtNativeField {
    const reader = this;

    while (reader.on_byte(FIELD_DATA_SEPARATOR_BYTE)) {
      reader.next();
    }

    if (reader.on_eof()) {
      throw new ParseBiFmtFieldUnexpectedEOFError(FIELD_START_BYTE);
    }

    if (!reader.on_byte(FIELD_START_BYTE)) {
      throw new ParseBiFmtFieldUnexpectedByteError(FIELD_START_BYTE, reader.cursor);
    }

    if (reader.next() == null) {
      throw new ParseBiFmtFieldUnexpectedEOFError();
    }

    switch (reader.cursor) {
      case FIELD_INT_KIND_BYTE: {
        reader.next();
        if (!reader.on_byte(FIELD_PIECE_SEPARATOR_BYTE)) {
          throw new ParseBiFmtFieldUnexpectedByteError(FIELD_PIECE_SEPARATOR_BYTE, reader.cursor);
        }

        const buffer = [] as number[];
        while (!reader.on_eof()) {
          const cursor = reader.next();
          if (cursor == null) {
            throw new ParseBiFmtFieldUnexpectedEOFError();
          }

          if (MIN_PRINTABLE_ASCII_TABLE_BYTE <= cursor && cursor <= MAX_PRINTABLE_ASCII_TABLE_BYTE) {
            buffer.push(cursor);
            continue;
          }

          if (cursor === FIELD_PIECE_SEPARATOR_BYTE) {
            break;
          }

          const expected_byte = buffer.length == 0 ? 69 /* 'E' */ : FIELD_PIECE_SEPARATOR_BYTE;

          throw new ParseBiFmtFieldUnexpectedByteError(expected_byte, cursor);
        }

        if (buffer.length == 0) {
          throw new ParseBiFmtFieldMissingNameError();
        }

        const name = new TextDecoder().decode(Uint8Array.from(buffer));

        if (!reader.on_byte(FIELD_PIECE_SEPARATOR_BYTE)) {
          throw new ParseBiFmtFieldUnexpectedByteError(FIELD_PIECE_SEPARATOR_BYTE, reader.cursor);
        }

        buffer.length = 0;
        while (!reader.on_eof()) {
          const cursor = reader.next();
          if (cursor == null) {
            throw new ParseBiFmtFieldUnexpectedEOFError(FIELD_INT_LITERAL_0_BYTE);
          }

          if (FIELD_INT_LITERAL_BYTES.includes(cursor as any)) {
            buffer.push(cursor);
            continue;
          }

          if (cursor === FIELD_DATA_SEPARATOR_BYTE) {
            break;
          }

          const expected_byte = buffer.length == 0 ? FIELD_INT_LITERAL_0_BYTE : FIELD_DATA_SEPARATOR_BYTE;

          throw new ParseBiFmtFieldUnexpectedByteError(expected_byte, cursor);
        }

        const value = Number(new TextDecoder().decode(Uint8Array.from(buffer)));

        return { kind: 'int', name, value };
      };

      case FIELD_BLOB_KIND_BYTE: {
        reader.next();
        if (!reader.on_byte(FIELD_PIECE_SEPARATOR_BYTE)) {
          throw new ParseBiFmtFieldUnexpectedByteError(FIELD_PIECE_SEPARATOR_BYTE, reader.cursor);
        }

        const buffer = [] as number[];
        while (!reader.on_eof()) {
          const cursor = reader.next();
          if (cursor == null) {
            throw new ParseBiFmtFieldUnexpectedEOFError();
          }

          if (MIN_PRINTABLE_ASCII_TABLE_BYTE <= cursor && cursor <= MAX_PRINTABLE_ASCII_TABLE_BYTE) {
            buffer.push(cursor);
            continue;
          }

          if (cursor === FIELD_PIECE_SEPARATOR_BYTE) {
            break;
          }

          const expected_byte = buffer.length == 0 ? 69 /* 'E' */ : FIELD_PIECE_SEPARATOR_BYTE;

          throw new ParseBiFmtFieldUnexpectedByteError(expected_byte, cursor);
        }

        if (buffer.length == 0) {
          throw new ParseBiFmtFieldMissingNameError();
        }

        const name = new TextDecoder().decode(Uint8Array.from(buffer));

        if (!reader.on_byte(FIELD_PIECE_SEPARATOR_BYTE)) {
          throw new ParseBiFmtFieldUnexpectedByteError(FIELD_PIECE_SEPARATOR_BYTE, reader.cursor);
        }

        buffer.length = 0;
        while (!reader.on_eof()) {
          const cursor = reader.next();
          if (cursor == null) {
            throw new ParseBiFmtFieldUnexpectedEOFError(FIELD_INT_LITERAL_0_BYTE);
          }

          if (FIELD_INT_LITERAL_BYTES.includes(cursor as any)) {
            buffer.push(cursor);
            continue;
          }

          if (cursor === FIELD_DATA_SEPARATOR_BYTE) {
            break;
          }

          const expected_byte = buffer.length == 0 ? FIELD_INT_LITERAL_0_BYTE : FIELD_DATA_SEPARATOR_BYTE;

          throw new ParseBiFmtFieldUnexpectedByteError(expected_byte, cursor);
        }

        const size = Number(new TextDecoder().decode(Uint8Array.from(buffer)));
        buffer.length = 0;

        const value = new Uint8Array(size);
        for (let i = 0; i < size; ++i) {
          const cursor = reader.next();
          if (cursor == null) {
            throw new ParseBiFmtFieldUnexpectedEOFError(FIELD_DATA_SEPARATOR_BYTE);
          }

          buffer.push(reader.cursor);
          value[i] = cursor;
        }

        return { kind: 'blob', name, value };
      };

      default: {
        throw new ParseBiFmtFieldInvalidKindError(reader.cursor);
      };
    }

  }


  try_read_field(): Result<BiFmtNativeField, ParseBiFmtFieldError> {
    try {
      return { ok: true, value: this.read_field() };
    } catch (e) {
      return { ok: false, error: e as ParseBiFmtFieldError };
    }
  }
}


export const create_reader = (initial_bytes: Bytes = []) => new BiFormatReader(initial_bytes);


export async function parse_bi_format_stream(stream: ReadableStream<Bytes>): Promise<Result<BiFmtStruct, ParseBiFmtFieldError>> {
  const stream_reader = stream.getReader();
  let step = await stream_reader.read();
  const bif_reader = create_reader(step.value ?? []);
  const struct = {} as BiFmtStruct;

  while (!step.done) {
    const bytes = step.value;
    bif_reader.append_bytes(bytes);
    bif_reader.save_point();

    const result = bif_reader.try_read_field();
    if (result.ok) {
      bif_reader.drop_save_point();
      const field = result.value;
      struct[field.name] = field;
      continue;
    }

    // If we hit an EOF, then we'll wait for more of the stream to hopefully read fields fine
    if (result.error instanceof ParseBiFmtFieldUnexpectedEOFError) {
      bif_reader.restore_point();
      continue;
    }

    // We stop reading as soon as we hit an error we can't recover
    return result;
  }

  while (!bif_reader.on_eof()) {
    const result = bif_reader.try_read_field();
    if (!result.ok) return result;
    const field = result.value;
    struct[field.name] = field;
  }

  return { ok: true, value: struct } as const;
}

export function* step_parse_bi_format(bytes: Bytes): Generator<BiFmtNativeField | null, Result<BiFmtStruct, ParseBiFmtFieldError>, Bytes | null | undefined> {
  const reader = create_reader(bytes);
  const struct = {} as BiFmtStruct;

  while (!reader.on_eof()) {
    reader.save_point();

    const result = reader.try_read_field();
    if (!result.ok) {
      if (result.error instanceof ParseBiFmtFieldUnexpectedEOFError) {
        const extra = yield null;
        if (extra != null) {
          reader.restore_point();
          reader.append_bytes(extra);
          continue;
        }
      }
      return result;
    }
    reader.drop_save_point();

    const field = result.value;
    if (field.name in struct) {
      const struct_field = struct[field.name]!;
      if (Array.isArray(struct_field)) {
        struct_field.push(field);
      } else {
        struct[field.name] = [struct_field, field];
      }
    } else {
      struct[field.name] = field;
    }

    const extra = yield field;
    if (extra != null) {
      reader.append_bytes(extra);
    }
  }

  return { ok: true, value: struct };
}

export function sync_parse_bi_format(bytes: Bytes) {
  const it = step_parse_bi_format(bytes);
  let step = it.next();
  while (!step.done) {
    step = it.next();
  }
  return step.value;
}

export function expect_bifmt_field_kind<Kind extends BiFmtFieldKind>(field: BiFmtNativeField, kind: Kind): Result<PickBiFmtField<Kind>, string> {
  if (kind === 'int' || kind === 'blob') {
    if (field.kind !== kind) {
      return { ok: false, error: `Field ${field.name} has kind ${field.kind} but expected ${kind}` };
    }

    return { ok: true, value: field as any };
  }

  if (kind == 'json' || kind == 'utf8') {
    if (field.kind !== 'blob') {
      return { ok: false, error: `Field ${field.name} has kind ${field.kind} but expected blob to be transformed to ${kind} ` };
    }
    let text: string;
    try {
      text = new TextDecoder().decode(field.value);
    } catch (e) {
      console.error(e);

      if (e instanceof Error) {
        return { ok: false, error: `Failed to parse blob as a utf-8 string: ${e.message}` };
      }

      return { ok: false, error: 'Failed to parse blob as a utf-8 string for an unknown reason' };
    }

    if (kind === 'utf8') {
      const v: BiFmtTransformedField<'utf8'> = { kind, name: field.name, value: text, buffer: field.value };
      return { ok: true, value: v as any };
    }

    try {
      const json = JSON.parse(text) as any;
      const v: BiFmtTransformedField<'json'> = { kind, name: field.name, value: json, buffer: field.value };
      return Result.Ok(v as any);
    } catch (e) {
      console.error(e);

      if (e instanceof Error) {
        return Result.Err(`Failed to parse utf8 blob as valid JSON: ${e.message}`);
      }

      return Result.Err('Failed to parse utf8 blob as valid JSON for an unknown reason');
    }
  }

  // if (kind == 'flt32') {
  //   if (field.kind !== 'blob') {
  //     return Result.Err('Field is not a set of bytes representing a 32 bit float');
  //   }
  //   const view = new DataView(field.value.buffer, field.value.byteOffset, field.value.byteLength);
  //   const value = view.getFloat32(0, true);
  //   const v: BiFmtTransformedField<'flt32'> = { kind, name: field.name, value, buffer: field.value };
  //   return Result.Ok(v as any);
  // }

  return { ok: false, error: `Unable to check if field is transformable to unsupported kind: ${kind}` };
}

function generate_int_field_bytes(value: number): Uint8Array {
  if (Number.isNaN(value)) throw new TypeError('Attempting to generate integer field bytes with NaN value');
  if (!Number.isFinite(value)) throw new TypeError('Attempting to generate integer field bytes with an Infinity value');
  const str_val = (Number.isInteger(value) ? value : Math.floor(value)).toString(10);
  let bytes = new Uint8Array(4 + str_val.length);
  let i = 0;
  bytes[i++] = FIELD_START_BYTE;
  bytes[i++] = FIELD_INT_KIND_BYTE;
  bytes[i++] = FIELD_PIECE_SEPARATOR_BYTE;
  for (const char of str_val) {
    let b: undefined | number;
    switch (char) {
      case '0': b = FIELD_INT_LITERAL_0_BYTE; break;
      case '1': b = FIELD_INT_LITERAL_1_BYTE; break;
      case '2': b = FIELD_INT_LITERAL_2_BYTE; break;
      case '3': b = FIELD_INT_LITERAL_3_BYTE; break;
      case '4': b = FIELD_INT_LITERAL_4_BYTE; break;
      case '5': b = FIELD_INT_LITERAL_5_BYTE; break;
      case '6': b = FIELD_INT_LITERAL_6_BYTE; break;
      case '7': b = FIELD_INT_LITERAL_7_BYTE; break;
      case '8': b = FIELD_INT_LITERAL_8_BYTE; break;
      case '9': b = FIELD_INT_LITERAL_9_BYTE; break;
    }
    if (b == undefined) continue;
    if (i >= bytes.length) {
      const n = Uint8Array.from([b]);
      bytes = concat_u8_to_bytes(bytes, n);
      continue;
    }
    bytes[i++] = b;
  }
  return concat_u8_to_bytes(bytes, [FIELD_DATA_SEPARATOR_BYTE]);
}

function generate_blob_field_bytes_for_string(str: string) {
  const encoder = new TextEncoder();
  const blob = encoder.encode(str);
  return generate_blob_field_bytes_for_bytes(blob);
}
function generate_blob_field_bytes_for_bytes(blob: Bytes) {
  const encoder = new TextEncoder();
  const length = encoder.encode(blob.length.toString(10));
  return concat_u8_to_bytes(
    Uint8Array.from([FIELD_START_BYTE, FIELD_BLOB_KIND_BYTE, FIELD_PIECE_SEPARATOR_BYTE]),
    length,
    [FIELD_DATA_SEPARATOR_BYTE],
    blob,
    [FIELD_DATA_SEPARATOR_BYTE],
  );
}

export function object_to_bi_format(obj: Record<string, unknown>): Result<Uint8Array, string> {
  const fields: Uint8Array[] = [];

  try {
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (v == null) {
        return Result.Err(`Invalid value for key ${k}, can not turn null/undefined to a value in BiF`);
      }

      if (typeof v == 'string') {
        const buf = generate_blob_field_bytes_for_string(v);
        fields.push(buf);
        continue;
      }

      if (typeof v == 'number') {
        if (Number.isInteger(v)) {
          const buf = generate_int_field_bytes(v);
          fields.push(buf);
          continue;
        }
      }

      if (Array.isArray(v)) {
        const str = JSON.stringify(v);
        const buf = generate_blob_field_bytes_for_string(str);
        fields.push(buf);
        continue;
      }

      if (typeof v == 'object') {
        const result = object_to_bi_format(v as any);
        if (!result.ok) {
          const str = JSON.stringify(v);
          const buf = generate_blob_field_bytes_for_string(str);
          fields.push(buf);
          continue;
        }
        const subbif = result.value;
        const buf = generate_blob_field_bytes_for_bytes(subbif);
        fields.push(buf);
        continue;
      }

      const str = JSON.stringify(v);
      const buf = generate_blob_field_bytes_for_string(str);
      fields.push(buf);
    }
  } catch (err) {
    return Result.Err(`Failed to serialize object: ${err}`);
  }

  return Result.Ok(
    fields.length == 0
      ? new Uint8Array(0)
      : fields.length == 1
        ? fields[0]!
        : concat_u8_to_bytes(new Uint8Array(0), ...(fields as [Uint8Array, ...Uint8Array[]]))
  );
}

