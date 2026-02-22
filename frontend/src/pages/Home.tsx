import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router";

import styles from "./Home.module.css";

export function Home() {
  const { t } = useTranslation();
  const navigate = useNavigate();

  return (
    <div className={styles.hero}>
      <h1 className={styles.title}>{t("app.title")}</h1>
      <p className={styles.tagline}>{t("home.tagline")}</p>
      <button className={styles.cta} onClick={() => navigate("/search")}>
        {t("home.cta")}
      </button>
    </div>
  );
}
