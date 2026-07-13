#!/usr/bin/env node
/** Общие константы GraphMind hooks (ESM). */

/** Пути правок, которые часто содержат кросс-проектные решения (→ scope: "global"). */
export const FRAMEWORK_PATH_RE =
  /(?:^|[\\/])\.cursor[\\/](?:rules[\\/]|hooks\.json)|(?:^|[\\/])\.kodik[\\/]rules[\\/]|(?:^|[\\/])mcp[\\/]graphmind|(?:^|[\\/])\.github[\\/]workflows[\\/]|(?:^|[\\/])AGENTS\.md$|(?:^|[\\/])KODIK\.md$|(?:^|[\\/])ARCHITECTURE\.md$/i;

export function isFrameworkPath(path) {
  if (!path) return false;
  return FRAMEWORK_PATH_RE.test(path.replace(/\\/g, "/"));
}
