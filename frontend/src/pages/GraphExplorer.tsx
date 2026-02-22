import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router";

import { Spinner } from "@/components/common/Spinner";
import { EntityDetail } from "@/components/entity/EntityDetail";
import { GraphCanvas } from "@/components/graph/GraphCanvas";
import { GraphControls } from "@/components/graph/GraphControls";
import { useGraphData } from "@/hooks/useGraphData";
import { entityColors } from "@/styles/tokens";

import styles from "./GraphExplorer.module.css";

const ALL_TYPES = new Set(Object.keys(entityColors));

export function GraphExplorer() {
  const { t } = useTranslation();
  const { entityId } = useParams<{ entityId: string }>();
  const [depth, setDepth] = useState(2);
  const [enabledTypes, setEnabledTypes] = useState<Set<string>>(ALL_TYPES);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);

  const { data, loading, error, refetch } = useGraphData(entityId, depth);

  const handleDepthChange = useCallback(
    (newDepth: number) => {
      setDepth(newDepth);
      const types = enabledTypes.size < ALL_TYPES.size ? [...enabledTypes] : undefined;
      refetch(newDepth, types);
    },
    [enabledTypes, refetch],
  );

  const handleToggleType = useCallback((type: string) => {
    setEnabledTypes((prev) => {
      const next = new Set(prev);
      if (next.has(type)) {
        next.delete(type);
      } else {
        next.add(type);
      }
      return next;
    });
  }, []);

  const handleNodeClick = useCallback((nodeId: string) => {
    setSelectedNodeId(nodeId);
  }, []);

  const handleCloseDetail = useCallback(() => {
    setSelectedNodeId(null);
  }, []);

  return (
    <div className={styles.explorer}>
      <div className={styles.sidebar}>
        <GraphControls
          depth={depth}
          onDepthChange={handleDepthChange}
          enabledTypes={enabledTypes}
          onToggleType={handleToggleType}
        />
      </div>

      <div className={styles.graphArea}>
        {loading && <Spinner />}
        {error && <p className={styles.status}>{error}</p>}
        {data && entityId && (
          <GraphCanvas
            data={data}
            centerId={entityId}
            enabledTypes={enabledTypes}
            onNodeClick={handleNodeClick}
          />
        )}
        {!loading && !data && !error && (
          <p className={styles.status}>{t("graph.noData")}</p>
        )}
      </div>

      {selectedNodeId && (
        <EntityDetail entityId={selectedNodeId} onClose={handleCloseDetail} />
      )}
    </div>
  );
}
