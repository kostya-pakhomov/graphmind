//! Тир-гейт open-core: по умолчанию сервер обслуживает локальную машину
//! (Free — сколько угодно агентов на том же хосте), публичные (маршрутизируемые)
//! источники отклоняются. Переход в командный/облачный тир — явным флагом
//! `GRAPHMIND_ALLOW_EXTERNAL=true`.
//!
//! Это МЯГКИЙ, честный маркер границы тира, не DRM: обходится (флаг, пересборка,
//! агент в соседнем контейнере). Надёжный слой «только локальная машина» —
//! публикация порта контейнера на loopback (docker-compose `GM_BIND=127.0.0.1`).
//! Оговорка: при Docker userland-proxy контейнер видит source как шлюз бриджа
//! для ВСЕГО трафика — тогда этот фильтр не различает локальное и внешнее;
//! именно поэтому основной гейт — bind на loopback, а не этот фильтр.

use std::net::IpAddr;

/// Прочитать `GRAPHMIND_ALLOW_EXTERNAL` (по умолчанию false).
pub fn allow_external_from_env() -> bool {
    std::env::var("GRAPHMIND_ALLOW_EXTERNAL")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

/// Разрешён ли источник. `allow_external=true` → разрешено всё.
/// Иначе разрешены loopback + приватные/локальные диапазоны (вкл. Docker-bridge и LAN),
/// а публичные (маршрутизируемые) адреса отклоняются.
pub fn is_source_allowed(ip: IpAddr, allow_external: bool) -> bool {
    allow_external || is_local_or_private(ip)
}

fn is_local_or_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()        // 127.0.0.0/8
                || v4.is_private()  // 10/8, 172.16/12 (Docker bridge), 192.168/16
                || v4.is_link_local() // 169.254/16
                || v4.is_unspecified() // 0.0.0.0
        }
        IpAddr::V6(v6) => {
            // IPv4-mapped (::ffff:a.b.c.d) — судим по встроенному v4.
            if let Some(m) = v6.to_ipv4_mapped() {
                return is_local_or_private(IpAddr::V4(m));
            }
            let seg0 = v6.segments()[0];
            v6.is_loopback()                 // ::1
                || v6.is_unspecified()       // ::
                || (seg0 & 0xfe00) == 0xfc00 // ULA fc00::/7
                || (seg0 & 0xffc0) == 0xfe80 // link-local fe80::/10
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn v4(s: &str) -> IpAddr {
        IpAddr::V4(s.parse::<Ipv4Addr>().unwrap())
    }

    #[test]
    fn loopback_and_private_allowed_by_default() {
        // allow_external=false
        assert!(is_source_allowed(v4("127.0.0.1"), false));
        assert!(is_source_allowed(v4("172.17.0.1"), false), "Docker bridge gateway");
        assert!(is_source_allowed(v4("172.18.0.1"), false), "compose network gateway");
        assert!(is_source_allowed(v4("10.1.2.3"), false));
        assert!(is_source_allowed(v4("192.168.1.50"), false), "LAN");
        assert!(is_source_allowed(IpAddr::V6(Ipv6Addr::LOCALHOST), false), "::1");
    }

    #[test]
    fn public_blocked_by_default() {
        assert!(!is_source_allowed(v4("8.8.8.8"), false));
        assert!(!is_source_allowed(v4("203.0.113.7"), false));
        // публичный IPv6 (документационный 2001:db8::) — маршрутизируемый
        assert!(!is_source_allowed(IpAddr::V6("2001:db8::1".parse().unwrap()), false));
    }

    #[test]
    fn allow_external_opens_everything() {
        assert!(is_source_allowed(v4("8.8.8.8"), true));
        assert!(is_source_allowed(v4("203.0.113.7"), true));
        assert!(is_source_allowed(IpAddr::V6("2001:db8::1".parse().unwrap()), true));
    }

    #[test]
    fn ipv4_mapped_v6_judged_by_inner_v4() {
        // ::ffff:127.0.0.1 → loopback → allowed; ::ffff:8.8.8.8 → public → blocked
        assert!(is_source_allowed(IpAddr::V6("::ffff:127.0.0.1".parse().unwrap()), false));
        assert!(!is_source_allowed(IpAddr::V6("::ffff:8.8.8.8".parse().unwrap()), false));
    }

    #[test]
    fn env_parse_truthy() {
        // sanity на парсер значений (без реального env)
        for t in ["1", "true", "TRUE", "yes", "on"] {
            std::env::set_var("GRAPHMIND_ALLOW_EXTERNAL", t);
            assert!(allow_external_from_env(), "{t} → true");
        }
        for f in ["0", "false", "", "no"] {
            std::env::set_var("GRAPHMIND_ALLOW_EXTERNAL", f);
            assert!(!allow_external_from_env(), "{f:?} → false");
        }
        std::env::remove_var("GRAPHMIND_ALLOW_EXTERNAL");
    }
}
