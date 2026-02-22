import { type ReactNode, useState } from "react";
import { useTranslation } from "react-i18next";
import { Link, useLocation } from "react-router";

import styles from "./AppShell.module.css";

export function AppShell({ children }: { children: ReactNode }) {
  const { t, i18n } = useTranslation();
  const location = useLocation();
  const [menuOpen, setMenuOpen] = useState(false);

  const toggleLang = () => {
    const next = i18n.language === "pt-BR" ? "en" : "pt-BR";
    i18n.changeLanguage(next);
  };

  const navLinkClass = (path: string) =>
    location.pathname.startsWith(path) ? styles.active : "";

  return (
    <div className={styles.shell}>
      <header className={styles.header}>
        <Link to="/" className={styles.logo}>
          {t("app.title")}
        </Link>
        <button
          className={styles.hamburger}
          onClick={() => setMenuOpen((prev) => !prev)}
          aria-label="Menu"
        >
          &#9776;
        </button>
        <nav className={`${styles.nav} ${menuOpen ? styles.navOpen : ""}`}>
          <Link
            to="/search"
            className={navLinkClass("/search")}
            onClick={() => setMenuOpen(false)}
          >
            {t("nav.search")}
          </Link>
          <Link
            to="/patterns"
            className={navLinkClass("/patterns")}
            onClick={() => setMenuOpen(false)}
          >
            {t("nav.patterns")}
          </Link>
          <Link
            to="/investigations"
            className={navLinkClass("/investigations")}
            onClick={() => setMenuOpen(false)}
          >
            {t("nav.investigations")}
          </Link>
        </nav>
        <button onClick={toggleLang} className={styles.langToggle}>
          {i18n.language === "pt-BR" ? "EN" : "PT"}
        </button>
      </header>
      <main className={styles.main}>{children}</main>
      <footer className={styles.footer}>
        <span className={styles.disclaimer}>{t("app.disclaimer")}</span>
      </footer>
    </div>
  );
}
