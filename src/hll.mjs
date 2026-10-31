// 1024-register HyperLogLog (p=10, 1 KB fixed memory) for Cloudflare Workers + D1

const P = 10;
const M = 1 << P; // 1024 registers
const ALPHA_1024 = 0.7213 / (1.0 + 1.079 / M);

function hash64(str) {
  let h = 14695981039346656037n;
  for (let i = 0; i < str.length; i++) {
    h ^= BigInt(str.charCodeAt(i));
    h = BigInt.asUintN(64, h * 1099511628211n);
  }
  h ^= h >> 33n;
  h = BigInt.asUintN(64, h * 0xff51afd7ed558ccdn);
  h ^= h >> 33n;
  h = BigInt.asUintN(64, h * 0xc4ceb9fe1a85ec53n);
  h ^= h >> 33n;
  return h;
}

export function createEmptyHLL() {
  return new Uint8Array(M);
}

export function hllAdd(registers, value) {
  const x = hash64(String(value));
  const idx = Number(x >> BigInt(64 - P));
  const remBits = 64 - P;
  const mask = (1n << BigInt(remBits)) - 1n;
  const w = x & mask;
  let rho = 1;
  for (let bit = remBits - 1; bit >= 0; bit--) {
    if (((w >> BigInt(bit)) & 1n) === 1n) {
      break;
    }
    rho++;
  }
  if (rho > registers[idx]) {
    registers[idx] = rho;
  }
}

export function hllCount(registers) {
  let sum = 0.0;
  let zeros = 0;
  for (let i = 0; i < M; i++) {
    const r = registers[i];
    sum += Math.pow(2.0, -r);
    if (r === 0) zeros++;
  }
  const rawEstimate = (ALPHA_1024 * M * M) / sum;
  if (rawEstimate <= 2.5 * M && zeros > 0) {
    return Math.round(M * Math.log(M / zeros));
  }
  return Math.round(rawEstimate);
}

export function serializeHLL(registers) {
  let binary = "";
  for (let i = 0; i < registers.length; i++) {
    binary += String.fromCharCode(registers[i]);
  }
  return btoa(binary);
}

export function deserializeHLL(b64) {
  if (!b64) return createEmptyHLL();
  const binary = atob(b64);
  const out = new Uint8Array(M);
  for (let i = 0; i < Math.min(M, binary.length); i++) {
    out[i] = binary.charCodeAt(i);
  }
  return out;
}
