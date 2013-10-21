(defproject net.async/async "0.1.0"
  :description "Network communications with clojure.core.async interface"
  :license {:name     "Eclipse Public License - v 1.0"
            :url      "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}
  :dependencies [
    [org.clojure/clojure "1.5.1"]
    [org.clojure/tools.logging "0.2.6"]
    [org.clojure/core.async "0.1.242.0-44b1e3-alpha"]
  ]
  :profiles {
    :dev {
      :global-vars   { *warn-on-reflection* true }
      :resource-dirs ["logs"]
      :jvm-opts [
        "-Djava.util.logging.config.file=logs/logging.properties"
      ]
    }
  }
)
