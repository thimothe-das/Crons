"use client";

import {
  Building2,
  Calendar,
  Filter,
  Home,
  LocationEdit,
  MapPin,
  Maximize2,
  Minimize2,
  ParkingCircle,
  Plus,
  TrendingUp,
  X,
} from "lucide-react";
import { useState } from "react";
import {
  Bar,
  BarChart,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { toast } from "sonner";

export default function DashboardPage() {
  const [loading, setLoading] = useState(false);
  const [newPostalCode, setNewPostalCode] = useState("");
  const [filters, setFilters] = useState({
    parcelle: "",
    type: "Appartement",
    minArea: "0",
    maxArea: "",
    garage: false,
    codesPostaux: [] as string[],
  });
  const [results, setResults] = useState<null | {
    nombre_transactions: number;
    prix_moyen: number;
    prix_median: number;
    prix_m2_moyen: number;
    prix_m2_median: number;
    transactions: Array<{
      date: string;
      prix: number;
      surface: number;
      prix_m2: number;
      adresse_nom_voie: string;
      commune: string;
      code_postal: string;
      adresse_complete: string;
      numero: string;
    }>;
    nombre_transactions_affiches: number;
  }>(null);

  // API URL
  const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:6644";

  const handleFilterChange = (
    key: string,
    value: string | boolean | string[]
  ) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
  };

  const handleAddPostalCode = () => {
    if (!newPostalCode) return;

    // Basic validation for French postal codes (5 digits)
    if (!/^\d{5}$/.test(newPostalCode)) {
      toast.error("Format invalide", {
        description: "Le code postal doit contenir 5 chiffres",
      });
      return;
    }

    // Check if already added
    if (filters.codesPostaux.includes(newPostalCode)) {
      toast.error("Code postal déjà ajouté", {
        description: `Le code postal ${newPostalCode} est déjà dans la liste`,
      });
      return;
    }

    // Add to list
    setFilters((prev) => ({
      ...prev,
      codesPostaux: [...prev.codesPostaux, newPostalCode],
    }));

    // Clear input
    setNewPostalCode("");
  };

  const handleRemovePostalCode = (codeToRemove: string) => {
    setFilters((prev) => ({
      ...prev,
      codesPostaux: prev.codesPostaux.filter((code) => code !== codeToRemove),
    }));
  };

  const handleAnalyze = async () => {
    setLoading(true);
    try {
      // Build query parameters based on filters
      const params = new URLSearchParams();

      if (filters.parcelle) {
        params.append("parcelles", filters.parcelle);
      }

      if (filters.type) {
        params.append("type", filters.type);
      }

      if (filters.minArea && filters.minArea !== "0") {
        params.append("min", filters.minArea);
      }

      if (filters.maxArea) {
        params.append("max", filters.maxArea);
      }

      if (filters.garage) {
        params.append("garage", "avec");
      }

      if (filters.codesPostaux.length > 0) {
        params.append("codes_postaux", filters.codesPostaux.join(","));
      }

      // Fetch data from the real API
      const url = `${API_URL}/api/dvf${
        params.toString() ? "?" + params.toString() : ""
      }`;
      const response = await fetch(url);

      if (!response.ok) {
        throw new Error(`API responded with status: ${response.status}`);
      }

      const data = await response.json();
      setResults(data);
      toast.success("Analyse terminée", {
        description: `${data.nombre_transactions} transactions trouvées (${data.nombre_transactions_affiches} affichées)`,
      });
    } catch (error) {
      console.error("Error fetching data:", error);
      toast.error("Erreur lors de l'analyse", {
        description:
          error instanceof Error ? error.message : "Une erreur est survenue",
      });
    } finally {
      setLoading(false);
    }
  };

  const priceChartData =
    results?.transactions.map((t) => ({
      date: t.date.split("/")[1] + "/" + t.date.split("/")[2].slice(2),
      prix: t.prix,
    })) || [];

  const pricePerM2ChartData =
    results?.transactions.map((t) => ({
      date: t.date.split("/")[1] + "/" + t.date.split("/")[2].slice(2),
      prix_m2: t.prix_m2,
    })) || [];

  const surfaceDistribution =
    results?.transactions.reduce((acc, t) => {
      const range = Math.floor(t.surface / 10) * 10;
      const key = `${range}-${range + 9}m²`;
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {} as Record<string, number>) || {};

  const surfaceChartData = Object.entries(surfaceDistribution).map(
    ([range, count]) => ({
      range,
      count,
    })
  );

  return (
    <div className="flex min-h-screen flex-col">
      <header className="sticky top-0 z-10 flex h-16 items-center gap-4 border-b bg-background px-4 md:px-6">
        <div className="flex items-center gap-2 font-semibold">
          <Building2 className="h-6 w-6" />
          <span>Analyse DVF - Prix Immobiliers</span>
        </div>
        <div className="ml-auto flex items-center gap-4">
          <Button
            variant="outline"
            size="sm"
            onClick={() => window.location.reload()}
          >
            Réinitialiser
          </Button>
        </div>
      </header>
      <div className="flex-1 grid grid-cols-1 md:grid-cols-[280px_1fr] lg:grid-cols-[280px_1fr]">
        <div className="border-r p-4 md:p-6">
          <div className="space-y-4">
            <div>
              <h3 className="mb-2 text-lg font-medium">Filtres</h3>
              <Separator />
            </div>
            <div className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="parcelle">ID Parcelle(s)</Label>
                <div className="flex items-center space-x-2">
                  <MapPin className="h-4 w-4 text-muted-foreground" />
                  <Input
                    id="parcelle"
                    placeholder="ex: 33281000BO0529,33281000BO0530"
                    value={filters.parcelle}
                    onChange={(e) =>
                      handleFilterChange("parcelle", e.target.value)
                    }
                  />
                </div>
                <p className="text-xs text-muted-foreground">
                  Vous pouvez saisir plusieurs IDs de parcelles séparés par des
                  virgules
                </p>
              </div>
              <div className="space-y-2">
                <Label htmlFor="codePostal">Codes Postaux</Label>
                <div className="flex items-center space-x-2">
                  <LocationEdit className="h-4 w-4 text-muted-foreground" />
                  <Input
                    id="codePostal"
                    placeholder="ex: 33000"
                    value={newPostalCode}
                    onChange={(e) => setNewPostalCode(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        handleAddPostalCode();
                      }
                    }}
                  />
                  <Button
                    type="button"
                    size="icon"
                    variant="outline"
                    onClick={handleAddPostalCode}
                  >
                    <Plus className="h-4 w-4" />
                  </Button>
                </div>

                {filters.codesPostaux.length > 0 && (
                  <div className="flex flex-wrap gap-2 mt-2">
                    {filters.codesPostaux.map((code) => (
                      <Badge
                        key={code}
                        variant="secondary"
                        className="flex items-center gap-1"
                      >
                        {code}
                        <button
                          type="button"
                          className="ml-1 rounded-full hover:bg-muted"
                          onClick={() => handleRemovePostalCode(code)}
                        >
                          <X className="h-3 w-3" />
                          <span className="sr-only">Supprimer</span>
                        </button>
                      </Badge>
                    ))}
                  </div>
                )}
              </div>
              <div className="space-y-2">
                <Label htmlFor="type">Type de bien</Label>
                <Select
                  value={filters.type}
                  onValueChange={(value) => handleFilterChange("type", value)}
                >
                  <SelectTrigger id="type">
                    <SelectValue placeholder="Sélectionner un type" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Appartement">Appartement</SelectItem>
                    <SelectItem value="Maison">Maison</SelectItem>
                    <SelectItem value="Dépendance">Dépendance</SelectItem>
                    <SelectItem value="Local industriel">
                      Local industriel
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label>Surface (m²)</Label>
                <div className="flex items-center space-x-2">
                  <div className="flex items-center space-x-2">
                    <Minimize2 className="h-4 w-4 text-muted-foreground" />
                    <Input
                      placeholder="Min"
                      className="w-20"
                      value={filters.minArea}
                      onChange={(e) =>
                        handleFilterChange("minArea", e.target.value)
                      }
                    />
                  </div>
                  <div className="flex items-center space-x-2">
                    <Maximize2 className="h-4 w-4 text-muted-foreground" />
                    <Input
                      placeholder="Max"
                      className="w-20"
                      value={filters.maxArea}
                      onChange={(e) =>
                        handleFilterChange("maxArea", e.target.value)
                      }
                    />
                  </div>
                </div>
              </div>

              <div className="flex items-center space-x-2">
                <Switch
                  id="garage"
                  checked={filters.garage}
                  onCheckedChange={(checked) =>
                    handleFilterChange("garage", checked)
                  }
                />
                <Label htmlFor="garage" className="flex items-center gap-2">
                  <ParkingCircle className="h-4 w-4" />
                  Avec garage
                </Label>
              </div>

              <Button
                className="w-full"
                onClick={handleAnalyze}
                disabled={loading}
              >
                {loading ? "Analyse en cours..." : "Analyser"}
              </Button>
            </div>
          </div>
        </div>
        <div className="p-4 md:p-6">
          {!results ? (
            <div className="flex h-full flex-col items-center justify-center">
              <div className="flex flex-col items-center justify-center space-y-4 text-center">
                <Home className="h-12 w-12 text-muted-foreground" />
                <div className="space-y-2">
                  <h3 className="text-xl font-medium">
                    Analyse des Prix Immobiliers
                  </h3>
                  <p className="text-muted-foreground">
                    Utilisez les filtres à gauche pour analyser les données DVF
                    et obtenir des statistiques sur les prix immobiliers.
                  </p>
                </div>
              </div>
            </div>
          ) : (
            <div className="space-y-6">
              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">
                      Transactions
                    </CardTitle>
                    <Filter className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">
                      {results.nombre_transactions}
                    </div>
                    <p className="text-xs text-muted-foreground">
                      {filters.codesPostaux.length > 0 && (
                        <>
                          {filters.codesPostaux.length === 1
                            ? `Code Postal: ${filters.codesPostaux[0]} • `
                            : `${filters.codesPostaux.length} Codes Postaux • `}
                        </>
                      )}
                      Biens correspondant aux critères
                    </p>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">
                      Prix moyen
                    </CardTitle>
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth="2"
                      className="h-4 w-4 text-muted-foreground"
                    >
                      <path d="M12 2v20M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6" />
                    </svg>
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">
                      {results.prix_moyen.toLocaleString("fr-FR")} €
                    </div>
                    <p className="text-xs text-muted-foreground">
                      {results.prix_m2_moyen.toLocaleString("fr-FR")} €/m²
                    </p>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">
                      Prix médian
                    </CardTitle>
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth="2"
                      className="h-4 w-4 text-muted-foreground"
                    >
                      <path d="M12 2v20M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6" />
                    </svg>
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">
                      {results.prix_median.toLocaleString("fr-FR")} €
                    </div>
                    <p className="text-xs text-muted-foreground">
                      {results.prix_m2_median.toLocaleString("fr-FR")} €/m²
                    </p>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">
                      Surface moyenne
                    </CardTitle>
                    <Maximize2 className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">
                      {Math.round(
                        results.transactions.reduce(
                          (sum, t) => sum + t.surface,
                          0
                        ) / results.transactions.length
                      )}{" "}
                      m²
                    </div>
                    <p className="text-xs text-muted-foreground">
                      {filters.minArea || "0"} - {filters.maxArea || "∞"} m²
                    </p>
                  </CardContent>
                </Card>
              </div>

              <Tabs defaultValue="price" className="space-y-4">
                <TabsList>
                  <TabsTrigger value="price">Prix</TabsTrigger>
                  <TabsTrigger value="price-m2">Prix au m²</TabsTrigger>
                  <TabsTrigger value="surface">Surfaces</TabsTrigger>
                </TabsList>
                <TabsContent value="price" className="space-y-4">
                  <Card>
                    <CardHeader>
                      <CardTitle>Évolution des prix</CardTitle>
                      <CardDescription>
                        Distribution des prix des {results.nombre_transactions}{" "}
                        transactions
                      </CardDescription>
                    </CardHeader>
                    <CardContent className="pl-2">
                      <ResponsiveContainer width="100%" height={350}>
                        <BarChart data={priceChartData}>
                          <XAxis
                            dataKey="date"
                            stroke="#888888"
                            fontSize={12}
                            tickLine={false}
                            axisLine={false}
                          />
                          <YAxis
                            stroke="#888888"
                            fontSize={12}
                            tickLine={false}
                            axisLine={false}
                            tickFormatter={(value) => `${value / 1000}k€`}
                          />
                          <Bar
                            dataKey="prix"
                            fill="hsl(var(--primary))"
                            radius={[4, 4, 0, 0]}
                          />
                        </BarChart>
                      </ResponsiveContainer>
                    </CardContent>
                  </Card>
                </TabsContent>
                <TabsContent value="price-m2" className="space-y-4">
                  <Card>
                    <CardHeader>
                      <CardTitle>Prix au m²</CardTitle>
                      <CardDescription>
                        Distribution des prix au m² des{" "}
                        {results.nombre_transactions} transactions
                      </CardDescription>
                    </CardHeader>
                    <CardContent className="pl-2">
                      <ResponsiveContainer width="100%" height={350}>
                        <BarChart data={pricePerM2ChartData}>
                          <XAxis
                            dataKey="date"
                            stroke="#888888"
                            fontSize={12}
                            tickLine={false}
                            axisLine={false}
                          />
                          <YAxis
                            stroke="#888888"
                            fontSize={12}
                            tickLine={false}
                            axisLine={false}
                            tickFormatter={(value) => `${value}€/m²`}
                          />
                          <Bar
                            dataKey="prix_m2"
                            fill="hsl(var(--primary))"
                            radius={[4, 4, 0, 0]}
                          />
                        </BarChart>
                      </ResponsiveContainer>
                    </CardContent>
                  </Card>
                </TabsContent>
                <TabsContent value="surface" className="space-y-4">
                  <Card>
                    <CardHeader>
                      <CardTitle>Distribution des surfaces</CardTitle>
                      <CardDescription>
                        Répartition des biens par tranche de surface
                      </CardDescription>
                    </CardHeader>
                    <CardContent className="pl-2">
                      <ResponsiveContainer width="100%" height={350}>
                        <BarChart data={surfaceChartData}>
                          <XAxis
                            dataKey="range"
                            stroke="#888888"
                            fontSize={12}
                            tickLine={false}
                            axisLine={false}
                          />
                          <YAxis
                            stroke="#888888"
                            fontSize={12}
                            tickLine={false}
                            axisLine={false}
                            tickFormatter={(value) => `${value}`}
                          />
                          <Bar
                            dataKey="count"
                            fill="hsl(var(--primary))"
                            radius={[4, 4, 0, 0]}
                          />
                        </BarChart>
                      </ResponsiveContainer>
                    </CardContent>
                  </Card>
                </TabsContent>
              </Tabs>

              <Card>
                <CardHeader>
                  <CardTitle>Détail des transactions</CardTitle>
                  <CardDescription>
                    Liste des {results.nombre_transactions} transactions
                    correspondant aux critères
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="rounded-md border">
                    <div className="grid grid-cols-7 border-b px-4 py-2 font-medium">
                      <div>Date</div>
                      <div>Prix</div>
                      <div>Surface</div>
                      <div>Prix/m²</div>
                      <div>Adresse</div>
                      <div>Commune</div>
                      <div>Code Postal</div>
                    </div>
                    <div className="divide-y">
                      {results.transactions.map((transaction, i) => (
                        <div key={i} className="grid grid-cols-7 px-4 py-3">
                          <div>{transaction.date}</div>
                          <div>
                            {transaction.prix.toLocaleString("fr-FR")} €
                          </div>
                          <div>{transaction.surface} m²</div>
                          <div>
                            {transaction.prix_m2.toLocaleString("fr-FR")} €/m²
                          </div>
                          <div>{transaction.adresse_complete}</div>
                          <div>{transaction.commune}</div>
                          <div>{transaction.code_postal}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
