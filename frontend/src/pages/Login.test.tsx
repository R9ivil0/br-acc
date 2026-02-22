import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

import "@/i18n";

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock("react-router", async () => {
  const actual = await vi.importActual<typeof import("react-router")>(
    "react-router",
  );
  return { ...actual, useNavigate: () => mockNavigate };
});

// Mock auth store
const mockLogin = vi.fn();
const mockRegister = vi.fn();
let mockStoreState = {
  login: mockLogin,
  register: mockRegister,
  loading: false,
  error: null as string | null,
  token: null as string | null,
};

vi.mock("@/stores/auth", () => ({
  useAuthStore: Object.assign(
    (selector?: (state: typeof mockStoreState) => unknown) =>
      selector ? selector(mockStoreState) : mockStoreState,
    {
      getState: () => mockStoreState,
    },
  ),
}));

import { Login } from "./Login";

function renderLogin() {
  return render(
    <MemoryRouter>
      <Login />
    </MemoryRouter>,
  );
}

describe("Login", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStoreState = {
      login: mockLogin,
      register: mockRegister,
      loading: false,
      error: null,
      token: null,
    };
  });

  it("renders login form by default", () => {
    renderLogin();

    expect(screen.getByLabelText(/e-mail/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/senha/i)).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /entrar/i }),
    ).toBeInTheDocument();
    // Invite code should not be visible
    expect(screen.queryByLabelText(/convite/i)).not.toBeInTheDocument();
  });

  it("renders register form when toggled", async () => {
    const user = userEvent.setup();
    renderLogin();

    await user.click(screen.getByText(/registre-se/i));

    expect(screen.getByLabelText(/e-mail/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/senha/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/convite/i)).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /registrar/i }),
    ).toBeInTheDocument();
  });

  it("submits login and calls store.login", async () => {
    const user = userEvent.setup();
    renderLogin();

    await user.type(screen.getByLabelText(/e-mail/i), "test@example.com");
    await user.type(screen.getByLabelText(/senha/i), "password123");
    await user.click(screen.getByRole("button", { name: /entrar/i }));

    expect(mockLogin).toHaveBeenCalledWith("test@example.com", "password123");
  });

  it("submits register and calls store.register", async () => {
    const user = userEvent.setup();
    renderLogin();

    // Switch to register mode
    await user.click(screen.getByText(/registre-se/i));

    await user.type(screen.getByLabelText(/e-mail/i), "new@example.com");
    await user.type(screen.getByLabelText(/senha/i), "password123");
    await user.type(screen.getByLabelText(/convite/i), "invite-abc");
    await user.click(screen.getByRole("button", { name: /registrar/i }));

    expect(mockRegister).toHaveBeenCalledWith(
      "new@example.com",
      "password123",
      "invite-abc",
    );
  });

  it("shows error from store", () => {
    mockStoreState.error = "auth.invalidCredentials";
    renderLogin();

    // The error is displayed via t(error), which resolves to translated text
    expect(
      screen.getByText(/e-mail ou senha incorretos/i),
    ).toBeInTheDocument();
  });

  it("disables submit button during loading", () => {
    mockStoreState.loading = true;
    renderLogin();

    const submitBtn = screen.getByRole("button", { name: /carregando/i });
    expect(submitBtn).toBeDisabled();
  });

  it("invite code field only visible in register mode", async () => {
    const user = userEvent.setup();
    renderLogin();

    // Not visible in login mode
    expect(screen.queryByLabelText(/convite/i)).not.toBeInTheDocument();

    // Switch to register
    await user.click(screen.getByText(/registre-se/i));
    expect(screen.getByLabelText(/convite/i)).toBeInTheDocument();

    // Switch back to login
    await user.click(screen.getByText(/já tem conta/i));
    expect(screen.queryByLabelText(/convite/i)).not.toBeInTheDocument();
  });

  it("navigates to /investigations on success", async () => {
    const user = userEvent.setup();

    // After login, token becomes set
    mockLogin.mockImplementation(() => {
      mockStoreState.token = "jwt-123";
      return Promise.resolve();
    });

    renderLogin();

    await user.type(screen.getByLabelText(/e-mail/i), "test@example.com");
    await user.type(screen.getByLabelText(/senha/i), "password123");
    await user.click(screen.getByRole("button", { name: /entrar/i }));

    expect(mockNavigate).toHaveBeenCalledWith("/investigations");
  });
});
